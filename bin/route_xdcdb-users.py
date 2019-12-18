#!/usr/bin/env python3

# Load XDCDB username information from a source (database) to a destination (warehouse)
import pdb
import django
import shutil
import ssl
import json
import psycopg2
import os
import pwd
import re
import sys
import argparse
import logging
import logging.handlers
import signal
import datetime
from datetime import datetime, tzinfo, timedelta
from time import sleep
import pytz
Central = pytz.timezone("US/Central")
UTC_TZ = pytz.utc

try:
    import http.client as httplib
except ImportError:
    import httplib

django.setup()
from processing_status.process import ProcessingActivity
from xdcdb.models import XSEDELocalUsermap
from django.utils.dateparse import parse_datetime
from django.db import DataError, IntegrityError


class UTC(tzinfo):
    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return 'UTC'

    def dst(self, dt):
        return timedelta(0)


utc = UTC()


class HandleLoad():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.dest = {}
        self.stats = {}
        for var in ['uri', 'scheme', 'path']:  # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None

        self.Affiliation = 'xsede.org'

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
        parser.add_argument('-c', '--config', action='store', default='./route_xdcdb-users.conf',
                            help='Configuration file default=./route_xdcdb-users.conf')

        parser.add_argument('--verbose', action='store_true',
                            help='Verbose output')
        parser.add_argument('--pdb', action='store_true',
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:
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
            print('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        # Initialize logging from arguments, or config file, or default to WARNING as last resort
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(
                logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'WARNING', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: {}'.format(numeric_log))
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(numeric_log)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], when='W6',
                                                                 backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
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

    def Retrieve_Usermap(self, cursor):
        try:
            sql = 'SELECT * from info_services.xsede_local_usermap'
            cursor.execute(sql)
        except psycopg2.Error as e:
            self.logger.error("Failed '{}' with {}: {}".format(
                sql, e.pgcode, e.pgerror))
            exit(1)

        COLS = [desc.name for desc in cursor.description]
        DATA = {}
        for row in cursor.fetchall():
            rowdict = dict(zip(COLS, row))
            DATA[str(rowdict['username'])+str(rowdict['resource_name'])] = rowdict
        return(DATA)

    def Warehouse_Usermap(self, new_items):
        self.cur = {}   # Items currently in database
        self.new = {}   # New resources in document
        now_utc = datetime.now(utc)

        for item in XSEDELocalUsermap.objects.all():
            if str(item.resource_name) in self.cur:
                if str(item.local_username) in self.cur[str(item.resource_name)]:
                    pass
                else:
                    self.cur[str(item.resource_name)][str(
                        item.local_username)] = item
            else:
                self.cur[str(item.resource_name)] = {
                    str(item.local_username): item}
        for new_id in new_items:
            nitem = new_items[new_id]
            if nitem['resource_name'] in self.cur:
                if nitem['username'] in self.cur[nitem['resource_name']]:
                    continue

            try:
                model = XSEDELocalUsermap(person_id=nitem['person_id'],
                                          portal_login=str(
                                              nitem['portal_login']),
                                          resource_id=nitem['resource_id'],
                                          resource_name=str(
                                              nitem['resource_name']),
                                          local_username=str(
                                              nitem['username']),
                                          ResourceID=str(
                                              nitem['resource_name'])+".org",

                                          )
                model.save()
                person_id = nitem['person_id']
                self.logger.debug(
                    'Usermap save person_id={}'.format(person_id))
                self.new[nitem['person_id']] = model
                self.stats['UserMap.Update'] += 1
            except (DataError, IntegrityError) as e:
                msg = '{} saving ID={}: {}'.format(
                    type(e).__name__, person_id, e.message)
                self.logger.error(msg)
                return(False, msg)

        for resource in self.cur:
            for local_user in self.cur[resource]:
                compstring = str(self.cur[resource][local_user].local_username)+str(self.cur[resource][local_user].resource_name)
                if compstring not in new_items:
                    try:
                        self.cur[resource][local_user].delete()
                        self.stats['Usermap.Delete'] += 1
                        self.logger.info('Usermap delete person_id={}'.format(
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
                    print('SaveDaemonLog as {}'.format(newpath))
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.critical('Caught signal={}, exiting...'.format(signal))
        sys.exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)
        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(
            __file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        while True:
            pa_application = os.path.basename(__file__)
            pa_function = 'Warehouse_Usermap'
            pa_id = 'xdcdb-usermap'
            pa_topic = 'Users'
            pa_about = 'xsede.org'
            pa = ProcessingActivity(
                pa_application, pa_function, pa_id, pa_topic, pa_about)

            if self.src['scheme'] == 'postgresql':
                CURSOR = self.Connect_Source(self.src['uri'])

            self.start = datetime.now(utc)
            self.stats['UserMap.Update'] = 0
            self.stats['UserMap.Delete'] = 0
            self.stats['UserMap.Skip'] = 0
            INPUT = self.Retrieve_Usermap(CURSOR)
            (rc, warehouse_msg) = self.Warehouse_Usermap(INPUT)
            self.end = datetime.now(utc)
            summary_msg = 'Processed UserMap in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(
            ), self.stats['UserMap.Update'], self.stats['UserMap.Delete'], self.stats['UserMap.Skip'])
            self.logger.info(summary_msg)

            self.Disconnect_Source(CURSOR)

            pa.FinishActivity(rc, summary_msg)
            break


if __name__ == '__main__':
    router = HandleLoad()
    myrouter = router.run()
    sys.exit(0)
