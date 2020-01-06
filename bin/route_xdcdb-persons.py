#!/usr/bin/env python3

# Load XDCDB Person information from a source (database) to a destination (warehouse)
import argparse
from datetime import datetime
import django
import json
import logging
import logging.handlers
import os
import psycopg2
import pwd
import re
import shutil
import signal
import sys

django.setup()
from processing_status.process import ProcessingActivity
from xdcdb.models import XSEDEPerson
from django.db import DataError, IntegrityError

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class HandleLoad():
    def __init__(self):
        self.MyName = 'Persons'

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
        parser.add_argument('-c', '--config', action='store', default='./route_xdcdb-persons.conf',
                            help='Configuration file default=./route_xdcdb-persons.conf')

        parser.add_argument('--verbose', action='store_true',
                            help='Verbose output')
        parser.add_argument('--pdb', action='store_true',
                            help='Run with Python debugger')
        self.args = parser.parse_args()

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
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        # Initialize logging from arguments, or config file, or default to WARNING as last resort
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'WARNING', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: {}'.format(numeric_log))
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(numeric_log)
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
            sys.exit(1)
        if self.src['scheme'] in ['http', 'https', 'postgresql']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                sys.exit(1)
            self.src['path'] = self.src['path'][2:]
        if len(self.src['path']) < 1:
            self.logger.error('Source is missing a database name')
            sys.exit(1)
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
            sys.exit(1)
        self.dest['uri'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error(
                'Source and Destination can not both be a {file}')
            sys.exit(1)

    def Connect_Source(self, url):
        idx = url.find(':')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)

        (type, obj) = (url[0:idx], url[idx+1:])
        if type not in ['postgresql']:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)

        if obj[0:2] != '//':
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)

        obj = obj[2:]
        idx = obj.find('/')
        if idx <= 0:
            self.logger.error('Retrieve URL is not valid')
            sys.exit(1)
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
            sql = 'SELECT * from info_services.address_v'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)
        COLS = [desc.name for desc in cursor.description]
        A_DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            key = rowdict['person_id']
            if key not in A_DATA:
                A_DATA[key] = []
            A_DATA[key].append(rowdict)
        
        try:
            sql = 'SELECT * from info_services.citizenship_v'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)
        COLS = [desc.name for desc in cursor.description]
        C_DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            key = rowdict['person_id']
            if key not in C_DATA:
                C_DATA[key] = rowdict['country']
            else:
                C_DATA[key] += ',' + rowdict['country']

        try:
            sql = 'SELECT * from info_services.email_v'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)
        COLS = [desc.name for desc in cursor.description]
        E_DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            key = rowdict['person_id']
            if key not in E_DATA:
                E_DATA[key] = rowdict['Email']
            else:
                E_DATA[key] += ',' + rowdict['Email']

        try:
            sql = 'SELECT * from info_services.person_v'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(sql, e.pgcode, e.pgerror))
            exit(1)
        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            key = rowdict['person_id']
            DATA[key] = rowdict
            DATA['addresses'] = A_DATA.get(key, {})
            DATA['citizenships'] = C_DATA.get(key, None)
            DATA['emails'] = E_DATA.get(key, None)
        return(DATA)

    def Store_Destination(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        now_utc = datetime.utcnow()

        for item in XSEDEPerson.objects.all():
            self.cur[item.person_id] = item
        for new_id in new_items:
            nitem = new_items[new_id]
            try:
                model = XSEDEPerson(person_id=nitem['person_id'],
                                      portal_login=str(nitem['portal_login']),
                                      last_name=str(nitem['last_name']),
                                      first_name=str(nitem['first_name']),
                                      middle_name=str(nitem['middle_name']),
                                      is_suspended=str(nitem['is_suspended']),
                                      organization=str(nitem['organization']),
                                      citizenships=str(nitem['citizenships']),
                                      emails=str(nitem['emails']),
                                      addressesJSON=nitem['addresses'],
                                  )
                model.save()
                person_id = nitem['person_id']
                self.logger.debug('Persons save person_id={}'.format(person_id))
                self.new[nitem['person_id']] = model
                self.MyUpdateStat += 1
            except (DataError, IntegrityError) as e:
                msg = '{} saving ID={}: {}'.format(
                    type(e).__name__, person_id, e.message)
                self.logger.error(msg)
                return(False, msg)

        for item in self.cur:
            if self.cur[item]['person_id'] not in new_items:
                try:
                    self.cur[item]['person_id'].delete()
                    self.MyDeleteStat += 1
                    self.logger.info('{} delete person_id={}'.format(self.MyName,
                        self.cur[resource][local_user].person_id))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting ID={}: {}'.format(
                        type(e).__name__, self.cur[resource][local_user].person_id, e.message))
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

    def exit_signal(self, signal, frame):
        self.logger.critical('Caught signal={}, exiting...'.format(signal))
        sys.exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(
            __file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        if self.src['scheme'] != 'postgresql':
            eprint('Source must be "postgresql"')
            sys.exit(1)
            
        while True:
            # Track that processing has started
            pa_application = os.path.basename(__file__)
            pa_function = 'Store_Destination'
            pa_id = 'xdcdb-persons'
            pa_topic = 'Persons'
            pa_about = 'xsede.org'
            pa = ProcessingActivity(pa_application, pa_function, pa_id, pa_topic, pa_about)

            self.start_ts = datetime.utcnow()
            self.MyUpdateStat = 0
            self.MyDeleteStat = 0
            self.MySkipStat = 0

            CURSOR = self.Connect_Source(self.src['uri'])
            INPUT = self.Retrieve_Source(CURSOR)
            (rc, warehouse_msg) = self.Store_Destination(INPUT)
            self.Disconnect_Source(CURSOR)

            self.end_ts = datetime.utcnow()
            summary_msg = 'Processed {} in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format(self.MyName,
                (self.end_ts - self.start_ts).total_seconds(), self.MyUpdateStat, self.MyDeleteStat, self.MySkipStat)
            self.logger.info(summary_msg)
            pa.FinishActivity(rc, summary_msg)
            break

if __name__ == '__main__':
    router = HandleLoad()
    myrouter = router.run()
    sys.exit(0)
