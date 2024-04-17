#!/usr/bin/env python3

# Router to synchronize ACCESS Allocations Fields of Science into the Information Sharing Platform
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
import logging.handlers
import os
from pid import PidFile
import psycopg2
import pwd
import re
import shutil
import signal
import sys, traceback

import django
django.setup()
from django.db import DataError, IntegrityError
from django.forms.models import model_to_dict
from allocations.models import FieldOfScience
from warehouse_state.process import ProcessingActivity

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class Router():
    # Initialization BEFORE we know if another self is running
    def __init__(self):
        parser = argparse.ArgumentParser(
            epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('-s', '--source', action='store', dest='src',
                            help='Messages source {postgresql} (default=postgresql)')
        parser.add_argument('-d', '--destination', action='store', dest='dest',
                            help='Message destination {analyze, or warehouse} (default=analyze)')
        parser.add_argument('--ignore_dates', action='store_true',
                            help='Ignore dates and force full resource refresh')

        parser.add_argument('-l', '--log', action='store',
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./router_accessdb-fos.conf',
                            help='Configuration file default=./router_accessdb-fos.conf')

        parser.add_argument('--verbose', action='store_true',
                            help='Verbose output')
        parser.add_argument('--pdb', action='store_true',
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        # Trace for debugging as early as possible
        if self.args.pdb:
            import pdb
            pdb.set_trace()

        # Load configuration file
        config_path = os.path.abspath(self.args.config)
        try:
            with open(config_path, 'r') as file:
                conf = file.read()
                file.close()
        except IOError as e:
            eprint('Error "{}" reading config={}'.format(e, config_path))
            sys.exit(1)
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        if self.config.get('PID_FILE'):
            self.pidfile_path =  self.config['PID_FILE']
        else:
            name = os.path.basename(__file__).replace('.py', '')
            self.pidfile_path = '/var/run/{}/{}.pid'.format(name, name)

    # Setup AFTER we know that no other self is running
    def Setup(self):
        # Initialize log level from arguments, or config file, or default to WARNING
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(loglevel_num)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(
            self.config['LOG_FILE'], when='W6', backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify nd parse source and destination arguments
        self.src = {}
        self.dest = {}
        for var in ['uri', 'scheme', 'path']:  # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None

        if not getattr(self.args, 'src', None):  # Tests for None and empty ''
            if 'SOURCE_URL' in self.config:
                self.args.src = self.config['SOURCE_URL']
        if not getattr(self.args, 'src', None):  # Tests for None and empty ''
            self.args.src = 'postgresql://localhost:5432/'
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['scheme'], self.src['path']) = (
                self.args.src[0:idx], self.args.src[idx+1:])
        else:
            (self.src['scheme'], self.src['path']) = (self.args.src, None)
        if self.src['scheme'] not in ['file', 'http', 'https', 'postgresql']:
            self.logger.error('Source not {file, http, https}')
            self.exit(1)
        if self.src['scheme'] in ['http', 'https', 'postgresql']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                self.exit(1)
            self.src['path'] = self.src['path'][2:]
        if len(self.src['path']) < 1:
            self.logger.error('Source is missing a database name')
            self.exit(1)
        self.src['uri'] = self.args.src

        if not getattr(self.args, 'dest', None):  # Tests for None and empty ''
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if not getattr(self.args, 'dest', None):  # Tests for None and empty ''
            self.args.dest = 'analyze'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['scheme'], self.dest['path']) = (
                self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['scheme'] = self.args.dest
        if self.dest['scheme'] not in ['file', 'analyze', 'warehouse']:
            self.logger.error('Destination not {file, analyze, warehouse}')
            self.exit(1)
        self.dest['uri'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error(
                'Source and Destination can not both be a {file}')
            self.exit(1)

        if self.src['scheme'] != 'postgresql':
            eprint('Source must be "postgresql"')
            self.exit(1)
            
        # Signal handling
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)

        self.ME = os.path.basename(__file__)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(self.ME,
            os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

    def Connect_Source(self, url):
        idx = url.find(':')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            self.exit(1)

        (type, obj) = (url[0:idx], url[idx+1:])
        if type not in ['postgresql']:
            self.logger.error('Retrieve URL is not valid')
            self.exit(1)

        if obj[0:2] != '//':
            self.logger.error('Retrieve URL is not valid')
            self.exit(1)

        obj = obj[2:]
        idx = obj.find('/')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            self.exit(1)
        (host, path) = (obj[0:idx], obj[idx+1:])
        idx = host.find(':')
        if idx > 0:
            port = host[idx+1:]
            host = host[:idx]
        elif type == 'postgresql':
            port = '5432'
        else:
            port = '5432'

        # Define our connection string
        conn_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(
            host, port, path, self.config['SOURCE_DBUSER'], self.config['SOURCE_DBPASS'])

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        self.logger.info('Connected to PostgreSQL database {} as {}'.format(
            path, self.config['SOURCE_DBUSER']))
        return(cursor)

    def Disconnect_Source(self, cursor):
        cursor.close()

    def Retrieve_Source(self, cursor):
        try:
            sql = 'SELECT * from info_services.fosv'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)
        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            key = rowdict['field_of_science_id']
            DATA[key] = rowdict
            na = DATA[key]['fos_nsf_abbrev']
            if isinstance(na, str) and na.lower() == 'none':
                DATA[key]['fos_nsf_abbrev'] = None
        return(DATA)

    def Store_Destination(self, new_items):
        self.cur = {}        # Items currently in database
        self.curdigest = {}  # Hashes for items currently in database
        self.curstring = {}  # String of items currently in database
        self.new = {}        # New resources in document
        now_utc = datetime.utcnow()

        for item in FieldOfScience.objects.all():
            self.cur[item.field_of_science_id] = item
            # Convert item to dict then string then calculate string hash
            # Optimize performance by only changing the database when hashes don't match
            xdict = model_to_dict(item)
            for i in xdict:
                if isinstance(xdict[i], str) and xdict[i].lower() == 'none':
                    xdict[i] = None
            sdict = {k:v for k,v in sorted(xdict.items())}
            strdict = str(sdict).encode('UTF-8')
            self.curstring[item.field_of_science_id] = strdict
            self.curdigest[item.field_of_science_id] = hashlib.md5(strdict).digest()
        for new_id in new_items:
            nitem = new_items[new_id]
            sdict = {k:v for k,v in sorted(nitem.items())}
            strdict = str(sdict).encode('UTF-8')
            if hashlib.md5(strdict).digest() == self.curdigest.get(new_id, ''):
                self.STATS.update({'Skip'})
                continue
            try:
                model, created = FieldOfScience.objects.update_or_create(
                                    field_of_science_id=nitem['field_of_science_id'],
                                    defaults = {
                                        'field_of_science_desc': str(nitem['field_of_science_desc']),
                                        'fos_nsf_id': nitem['fos_nsf_id'],
                                        'fos_nsf_abbrev': str(nitem['fos_nsf_abbrev']),
                                        'is_active': str(nitem['is_active']),
                                        'fos_source': str(nitem['fos_source']),
                                        'nsf_directorate_id': str(nitem['nsf_directorate_id']),
                                        'nsf_directorate_name': str(nitem['nsf_directorate_name']),
                                        'nsf_directorate_abbrev': str(nitem['nsf_directorate_abbrev']),
                                        'parent_field_of_science_id': nitem['parent_field_of_science_id'],
                                        'parent_field_of_science_desc': nitem['parent_field_of_science_desc'],
                                        'parent_fos_nsf_id': nitem['parent_fos_nsf_id'],
                                        'parent_fos_nsf_abbrev': str(nitem['parent_fos_nsf_abbrev'])
                                    })
                model.save()
                field_of_science_id = nitem['field_of_science_id']
                self.logger.debug('FOS save field_of_science_id={}'.format(field_of_science_id))
                self.new[nitem['field_of_science_id']] = model
                self.STATS.update({'Update'})
            except (DataError, IntegrityError) as e:
                msg = '{} saving ID={}: {}'.format(
                    type(e).__name__, nitem['field_of_science_id'], str(e))
                self.logger.error(msg)
                return(False, msg)

        for cur_id in self.cur:
            if cur_id not in new_items:
                try:
                    self.cur[cur_id].delete()
                    self.STATS.update({'Delete'})
                    self.logger.info('{} delete field_of_science_id={}'.format(self.ME,
                        self.cur[cur_id].field_of_science_id))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(
                        type(e).__name__, self.cur[cur_id].field_of_science_id, str(e)))
        return(True, '')

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines = file.read()
                file.close()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '{}.{}'.format(path, ts)
                    shutil.copy(path, newpath)
                    eprint('SaveDaemonLog as {}'.format(newpath))
        except Exception as e:
            eprint('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signum, frame):
        self.logger.critical('Caught signal={}({}), exiting with rc={}'.format(signum, signal.Signals(signum).name, signum))
        sys.exit(signum)
        
    def exit(self, rc):
        if rc:
            self.logger.error('Exiting with rc={}'.format(rc))
        sys.exit(rc)

    def Run(self):
        while True:
            self.start = datetime.now(timezone.utc)
            self.STATS = Counter()
            # Track that processing has started
            pa_application = os.path.basename(__file__)
            pa_function = 'Store_Destination'
            pa_id = 'allocations-fos'
            pa_topic = 'FOS'
            pa_about = 'access-ci.org'
            pa = ProcessingActivity(pa_application, pa_function, pa_id, pa_topic, pa_about)

            CURSOR = self.Connect_Source(self.src['uri'])
            INPUT = self.Retrieve_Source(CURSOR)
            (rc, warehouse_msg) = self.Store_Destination(INPUT)
            self.Disconnect_Source(CURSOR)

            self.end = datetime.now(timezone.utc)
            summary_msg = 'Processed {} in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format(self.ME,
                (self.end - self.start).total_seconds(), self.STATS['Update'], self.STATS['Delete'], self.STATS['Skip'])
            self.logger.info(summary_msg)
            pa.FinishActivity(rc, summary_msg)
            break

if __name__ == '__main__':
    router = Router()
    with PidFile(router.pidfile_path):
        try:
            router.Setup()
            rc = router.Run()
        except Exception as e:
            msg = '{} Exception: {}'.format(type(e).__name__, str(e))
            router.logger.error(msg)
            traceback.print_exc(file=sys.stdout)
            rc = 1
    router.exit(rc)
