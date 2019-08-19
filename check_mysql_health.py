#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Patrick Kummutat"
__version__ = "0.6"
__date__ = "15/11/2018"

import argparse
import sys
import MySQLdb
from math import log


MYSQL_HOST_NOT_ALLOWED = 1130
MYSQL_UNKOWN_HOST = 2005
MYSQL_ACCESS_DENIED = 1045
MYSQL_REPLICATION_SLAVE_PRIV = 1227
MYSQL_RESULT_FETCH_ALL = "ALL"
MYSQL_RESULT_FETCH_ONE = "ONE"


def pretty_size(n, pow=0, b=1024, u='B', pre=[''] + [p+'i'for p in'KMGTPEZY']):
    pow, n=min(int(log(max(n*b**pow,1), b)),len(pre)-1), n*b**pow

    return "%%.%if %%s%%s"%abs(pow%(-pow-1))%(n/b**float(pow), pre[pow], u)


def pretty_time(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    if days > 0:
        return '%s%dd%dh%dm%ds' % (sign_string, days, hours, minutes, seconds)

    elif hours > 0:
        return '%s%dh%dm%ds' % (sign_string, hours, minutes, seconds)

    elif minutes > 0:
        return '%s%dm%ds' % (sign_string, minutes, seconds)

    else:
        return '%s%ds' % (sign_string, seconds)


class MySQLServerConnectException(Exception):
    pass


class MySQLServer():
    state_ok = 0
    state_warning = 1
    state_critical = 2
    state_unknown = 3

    def __init__(self, kwargs):
        """
        @params kwargs: connection details for mysql
        @type: dict
        @raise MySQLServerConnectException: When mysql connect fails
        """

        self._kwargs = kwargs
        self._state = MySQLServer.state_ok
        self._messages = dict(ok=list(),
                              warning=list(),
                              critical=list())

        self._perf_data = list()
        self._mysql = dict()
        self._connection = None
        self._is_slave = False
        self._master = None

        try:
            self._connection = MySQLdb.connect(**kwargs)
            self._cursor = self._connection.cursor(
                    cursorclass=MySQLdb.cursors.DictCursor)

        except Exception, e:
            raise MySQLServerConnectException(e)

        self._global_variables()
        self._global_status()
        self._slave_status()


    def __exit__(self, type, value, traceback):

        if self._connection:
            self._connection.close()


    def _set_state(self, state):
        """
        @param state:
        @type: int
        """

        if state >= self._state:
            self._state = state


    def _connect_failed(self):
        raise Exception


    def _run_query(self, sql, fetch=MYSQL_RESULT_FETCH_ALL):
        """
        @param sql: any valid sql to execute
        @type: string
        @param fetch: returns all fetched rows or
                      just one line
        @type: string
        """

        self._cursor.execute(sql)

        if fetch == MYSQL_RESULT_FETCH_ONE:
            return self._cursor.fetchone()

        else:
            return self._cursor.fetchall()


    def _print_status(self, level):
        """
        helper function to print the monitoring status
        and performance data

        @param level: expects ok, warning or critical
                      to nicely format the output
        @type: string
        """

        msg = "Ok Database Health"
        if level == 'ok':
            print(msg)

        else:
            msg =  "{} Database Health - {}"
            print(msg.format(level.capitalize(),
                         self._messages[level].pop(0)))

        for msg in self._messages[level.lower()]:
            print("{} - {}".format(level.capitalize(), msg))

        print("|{}".format(' '.join(self._perf_data)))


    def _slave_status(self):
        """
        gather slave status information
        """

        status = self._run_query("SHOW SLAVE STATUS")

        if len(status) > 0:
            self._mysql['slave'] = status[0]
            self._is_slave = True


    def _slave_hosts(self):
        """
        gather slave host count

        @returns: connected slaves
        @returntype: int
        """

        slaves = self._run_query("SHOW SLAVE HOSTS")

        return len(slaves)


    def _master_status(self):
        """
        gather show master status output

        @returns: file, position, binlog_do_db and binlog_ignore_db
        @returntype: dict
        """

        return self._run_query("SHOW MASTER STATUS", MYSQL_RESULT_FETCH_ONE)


    def _master_logs(self):
        """
        gather show master logs output

        @returns: log_name and file_size
        @returntype: list of dicts
        """

        return self._run_query("SHOW MASTER LOGS")


    def _global_status(self):
        """
        gather global status values
        """

        self._mysql['status'] = dict()
        for row in self._run_query("SHOW GLOBAL STATUS"):
            self._mysql['status'].update({row['Variable_name']:row['Value']})


    def _global_variables(self):
        """
        gather global variable values
        """

        self._mysql['variables'] = dict()
        for row in self._run_query("SHOW GLOBAL VARIABLES"):
            self._mysql['variables'].update({row['Variable_name']:row['Value']})


    def check_threads_usage(self, warning, critical):
        """
        calculate thread usage in percentage
        proportionally to the innodb_thread_concurrency setting

        @param warning: threshold in percentage
        @type warning: int or float
        @param critical: threshold in percentage
        @type warning: int or float
        """

        threads_running = float(self._mysql['status'].get('Threads_running', 0))
        thread_concurrency = float(self._mysql['variables'].get('innodb_thread_concurrency'))

        # Skip thread checking when concurrency is set unlimited
        unlimited = 0
        if thread_concurrency == unlimited:
            return

        thread_usage = round((threads_running / thread_concurrency) * 100.0, 2)

        perf_data = "thread_usage={}%;{};{}".format(thread_usage, warning, critical)
        self._perf_data.append(perf_data)

        msg = "Thread usage {}% "\
              "Threads running {} "\
              "Thread concurrency {} ".format(thread_usage,
                                              threads_running,
                                              thread_concurrency)

        if thread_usage >= critical:
            self._messages['critical'].append(msg)
            self._set_state(MySQLServer.state_critical)

        elif thread_usage >= warning:
            self._messages['warning'].append(msg)
            self._set_state(MySQLServer.state_warning)

        else:
            self._messages['ok'].append(msg)


    def check_connections(self, warning, critical):
        """
        calculate connection usage in percentage

        @param warning: threshold in percentage
        @type warning: int or float
        @param critical: threshold in percentage
        @type warning: int or float
        """

        threads_connected = float(self._mysql['status'].get('Threads_connected', 0))
        max_connections = float(self._mysql['variables'].get('max_connections', 0))
        connection_usage = round((threads_connected/max_connections)*100, 2)

        perf_data = "connection_usage={}%;{};{}".format(connection_usage,
                                                        warning,
                                                        critical)
        self._perf_data.append(perf_data)

        msg = "Connections used {}% "\
              "Threads connected {} "\
              "Max connections {}".format(connection_usage,
                                          threads_connected,
                                          max_connections)

        if connection_usage >= critical:
            self._messages['critical'].append(msg)
            self._set_state(MySQLServer.state_critical)

        elif connection_usage >= warning:
            self._messages['warning'].append(msg)
            self._set_state(MySQLServer.state_warning)

        else:
            self._messages['ok'].append(msg)


    def _connect_master(self):
        """
        open a connection to master host
        using same credentials as for checking host
        just replacing host and port from slave status
        """

        # Copy connection args
        master_connection = self._kwargs
        # Replacing host and port with values from slave status
        master_connection['host'] = self._mysql['slave'].get('Master_Host')
        master_connection['port'] = self._mysql['slave'].get('Master_Port')

        try:
            self._master = MySQLServer(master_connection)
        except Exception, e:
            # failed open connection on master server
            pass


    def _diff_binlog_master_slave(self, slave_status_only=False):
        """
        calculate byte offset between master and slave

        @param slave_status_only: indicate to useshow slave status is used
        @type: bool

        @returns: bytes offset between master and slave
        @returntype: int
        """

        master_log_ahead = 0
        lag_bytes = 0
        slave_logfile_matched = False
        slave_relay_master_log_file = self._mysql['slave'].get('Relay_Master_Log_File')
        slave_master_log_file = self._mysql['slave'].get('Master_Log_File')
        slave_exec_master_log_pos = self._mysql['slave'].get('Exec_Master_Log_Pos')

        if not slave_status_only:
            for log in self._master._master_logs():

               if log.get('Log_name') == slave_relay_master_log_file or \
                  slave_logfile_matched:

                  slave_logfile_matched = True
                  master_log_ahead = master_log_ahead + int(log.get('File_size'))

            lag_bytes = master_log_ahead - slave_exec_master_log_pos

        else:
            # Fallback method to estimate the byte lag
            # Assume the slave has the same setting for max_binlog_size as master
            max_binlog_size = self._mysql['variables'].get('max_binlog_size')
            # Extract number from binlog file names
            master_logfile_nr = int(slave_master_log_file.split('.')[1])
            slave_logfile_nr = int(slave_relay_master_log_file.split('.')[1])
            # Calculate offset based on current logfile applied on slave
            logfile_nr_offset = master_logfile_nr - slave_logfile_nr

            if logfile_nr_offset > 0:
                lag_bytes = logfile_nr_offset * max_binlog_size - (slave_exec_master_log_pos)

        return lag_bytes


    def _get_replication_lag(self):
        """
        try connecting to master to calculate byte offset from slave

        @returns: byte offset
        @returntype: int
        """

        lag_bytes = 0

        self._connect_master()

        # Use data from master to calculate lag instead of slave status output
        if self._master:
            lag_bytes = self._diff_binlog_master_slave()
        else:
            lag_bytes = self._diff_binlog_master_slave(slave_status_only=True)

        return lag_bytes


    def check_replication(self, threshold_seconds_warning, threshold_seconds_critical,
                          threshold_bytes_warning, threshold_bytes_critical,
                          ignore_readonly_warning=False):
        """
        examine the replication status of a slave

        @param threshold_seconds: warning and critical threshold in seconds
        @type warning: dict
        @param threshold_bytes: warning and critical threshold in bytes
        @type critical: dict
        """

        if self._is_slave:
            read_only = self._mysql['variables'].get('read_only')
            lag_seconds = self._mysql['slave'].get('Seconds_Behind_Master', 0)
            slave_sql_thread = self._mysql['slave'].get('Slave_SQL_Running', 'No')
            slave_io_thread = self._mysql['slave'].get('Slave_IO_Running', 'No')
            slave_err = self._mysql['slave'].get('Last_Errno', '')
            master_host = self._mysql['slave'].get('Master_Host')
            master_port = self._mysql['slave'].get('Master_Port')

            lag_bytes = self._get_replication_lag()
            perf_msg = "replication_lag_bytes={}b;{};{}"
            self._perf_data.append(perf_msg.format(lag_bytes,
                                                   threshold_bytes_warning,
                                                   threshold_bytes_critical))

            if lag_seconds is None:
                lag_seconds = -1

            perf_msg = "replication_lag_seconds={}s;{};{}"
            self._perf_data.append(perf_msg.format(lag_seconds,
                                                   threshold_seconds_warning,
                                                   threshold_seconds_critical))

            if slave_sql_thread != 'Yes':
                msg = "Replication SQL Thread is down"
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

                if slave_err:
                    msg = "Last Error: {}".format(slave_err)
                    self._messages['critical'].append(msg)

            if slave_io_thread != 'Yes':
                msg = "Replication IO Thread is down"
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            if not ignore_readonly_warning and read_only != 'ON':
                msg = "Slave is not operating in read only mode"
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            msg = "Replication "\
                  "Master {}:{} "\
                  "Slave lag {}/{}".format(
                                           master_host,
                                           master_port,
                                           pretty_time(lag_seconds),
                                           pretty_size(lag_bytes)
                                          )

            if lag_bytes >= threshold_bytes_critical:
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            elif lag_bytes >= threshold_bytes_warning:
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            else:
                pass

            if lag_seconds >= threshold_seconds_critical:
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            elif lag_seconds >= threshold_seconds_warning:
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            else:
                self._messages['ok'].append(msg)


    def check_slave_connections(self, warning, critical):
        """
        check connected slave hosts

        @param warning: threshold in seconds
        @type warning: int or float
        @param critical: threshold in seconds
        @type critical: int or float
        """

        if warning == -1 or critical == -1:
            return

        slaves = self._slave_hosts()

        self._perf_data.append("slaves_connected={};{};{}".format(slaves,
                                                                   warning,
                                                                   critical))
        msg = "Slave connected {}".format(slaves)

        if slaves <= critical:
            self._messages['critical'].append(msg)
            self._set_state(MySQLServer.state_critical)

        elif slaves <= warning:
            self._messages['warning'].append(msg)
            self._set_state(MySQLServer.state_warning)

        else:
            self._messages['ok'].append(msg)


    def check_user_connections(self, username, alert_level, warning, critical):
        """
        checks connected user filtered by username
        per default alert level is set to warning and
        critical threshold is ignored,
        except when alert level is set to critical

        @param username: filter connections by username
        @type: str
        @param alertlevel: notice level for alerts
        @type: str
        @param warning: threshold for warnings
        @type: int
        @param critical: threshold for criticals
        @type: int
        """

        query = "SELECT COUNT(*) AS count " \
                "FROM information_schema.processlist " \
                "WHERE User NOT IN ('system user')"

        if username:
            query += " AND User = '{}'".format(username)

        user = self._run_query(query,
                               MYSQL_RESULT_FETCH_ONE)

        perf_msg = "user_connected={};{};{}"
        self._perf_data.append(perf_msg.format(user.get('count'),
                                               warning,
                                               critical))

        msg = "User {} has {} connections (w:{} c:{})".format(username,
                                                              user.get('count'),
                                                              warning,
                                                              critical)

        if user.get('count') <= critical and alert_level == 'critical':
            self._messages['critical'].append(msg)
            self._set_state(MySQLServer.state_critical)

        elif user.get('count') <= warning:
            self._messages['warning'].append(msg)
            self._set_state(MySQLServer.state_warning)

        else:
            self._messages['ok'].append(msg)


    def check_liquibase(self, database, table, warning, critical):
        """
        checks held locks in liquibase for a certain time
        and alerts if threshold is reached

        @param database: database to lookup for lock table
        @type: str
        @param table: name of the lock table
        @type: str
        @param warning: warning threshold
        @type: int
        @param critical: critical threshold
        @type: int
        """

        exclude_database = [
                             "'mysql'",
                             "'information_schema'",
                             "'performance_schema'",
                           ]

        query = "SELECT TABLE_NAME, TABLE_SCHEMA FROM "\
                "information_schema.TABLES WHERE "\
                "TABLE_SCHEMA NOT IN ({}) AND "\
                "TABLE_NAME = '{}'".format(
                                           ','.join(exclude_database),
                                           table
                                          )

        locks = []
        lock_tables = []

        if database:
            query += " AND TABLE_SCHEMA = '{}'".format(database)

        if table:
            query += " AND TABLE_NAME = '{}'".format(table)

        lock_tables = self._run_query(query, MYSQL_RESULT_FETCH_ALL)

        if lock_tables:
            for lock in lock_tables:
                query = "SELECT LOCKGRANTED, LOCKEDBY, "\
                        "(UNIX_TIMESTAMP()-UNIX_TIMESTAMP(LOCKGRANTED)) AS SECONDS "\
                        "FROM `{}`.`{}` "\
                        "WHERE LOCKGRANTED IS NOT NULL".format(
                                                         lock.get('TABLE_SCHEMA'),
                                                         lock.get('TABLE_NAME')
                                                        )

                result = self._run_query(query, MYSQL_RESULT_FETCH_ONE)

                if result:
                   db = { 'DATABASE': lock.get('TABLE_SCHEMA') }
                   result.update(db)
                   locks.append(result)

        else:
            msg = "Liquibase lock table {} not found"
            self._messages['warning'].append(msg.format(table))
            self._set_state(MySQLServer.state_warning)

        msg = "Liquibase lock held by {} in {} for {} seconds"
        for lock in locks:
            if lock.get('SECONDS') >= critical:
                self._messages['critical'].append(msg.format(
                                                             lock.get('LOCKEDBY'),
                                                             lock.get('DATABASE'),
                                                             lock.get('SECONDS')
                                                            ))

                self._set_state(MySQLServer.state_critical)

            if lock.get('SECONDS') >= warning:
                self._messages['warning'].append(msg.format(
                                                            lock.get('LOCKEDBY'),
                                                            lock.get('DATABASE'),
                                                            lock.get('SECONDS')
                                                           ))

                self._set_state(MySQLServer.state_warning)

        if self._state == MySQLServer.state_ok:
            msg = "Liquibase locks"
            self._messages['ok'].append(msg)


    def check_heartbeat(self, table, column):
        """
        Updates unix timestamp in heartbeat table

        @param table: name of the heartbeat table
        @type: str
        @param column: name of column to update
        @type: str
        """

        if self._mysql['variables'].get('read_only') != 'ON':
            msg = "Heartbeat"
            try:
                delete = "DELETE FROM {table}".format(table=table)
                insert = "INSERT INTO {table}({column}) VALUES(UNIX_TIMESTAMP())"
                insert = insert.format(table=table, column=column)

                self._cursor.execute(delete)
                self._cursor.execute(insert)
                self._connection.commit()

            except Exception as e:
                msg = "{} failed to update unix timestamp ({})".format(msg, e[1])
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            self._messages['ok'].append(msg)
            self._set_state(MySQLServer.state_ok)


    def check_definer(self, targets):
        """
        Check for none existing definers in routines,
        triggers, events and views

        @param targets: targets to check for broken definers
        """

        broken = {x: {} for x in targets}

        def add_broken(user, host, target):
            if user in broken[target]:
                broken[target][user].append(host)
            else:
                broken[target][user] = [host]

        query = "SELECT User,Host FROM mysql.user"
        users = {}

        for row in self._run_query(query):
            if row['User'] in users:
                users[row['User']].append(row['Host'])
            else:
                users[row['User']] = [row['Host']]

        query = "SELECT SUBSTRING_INDEX(DEFINER, '@', '1') AS User,"\
                "SUBSTRING_INDEX(DEFINER,'@',-1) AS Host "\
                "FROM information_schema.{} "\
                "GROUP BY User, Host"

        for target in targets:
            for row in self._run_query(query.format(target.upper())):

                if row['User'] in users:
                    if row['Host'] not in users[row['User']]:
                        add_broken(row['User'], row['Host'], target)
                else:
                    add_broken(row['User'], row['Host'], target)

        msg = "Definer for {}".format('/'.join(targets))
        self._messages['ok'].append(msg)

        for target in targets:

           if len(broken[target]) > 0:
               msg = "Definer [{}] in {} is broken".format(broken[target],target)
               self._messages['warning'].append(msg)
               self._set_state(MySQLServer.state_warning)


    def status(self, check):
        """
        calls all check functions and generate
        the status output

        @param check: threshold for each check command
        @returns: the exit code for icinga
        @returntype: int
        """

        if check['check_heartbeat']:
            self.check_heartbeat(
                table=check['heartbeat_table'],
                column=check['heartbeat_column']
            )

        if check['check_replication']:
            self.check_replication(
                check['replication_lag_seconds_warning'],
                check['replication_lag_seconds_critical'],
                check['replication_lag_bytes_warning'],
                check['replication_lag_bytes_critical'],
                check['replication_ignore_readonly_warning']
            )

        if check['check_threads']:
            self.check_threads_usage(
                warning=check['threads_warning'],
                critical=check['threads_critical']
            )

        if check['check_user_connections']:
            self.check_user_connections(
                username=check['user_connections_filter'],
                alert_level=check['user_connections_max_alertlevel'],
                warning=check['user_connections_warning'],
                critical=check['user_connections_critical']
            )

        if check['check_connections']:
            self.check_connections(
                warning=check['connections_warning'],
                critical=check['connections_critical'],
            )

        if check['check_slave_connections']:
            self.check_slave_connections(
                warning=check['slave_connections_warning'],
                critical=check['slave_connections_critical'],
            )

        if check['check_liquibase']:
            self.check_liquibase(
                database=check['liquibase_database'],
                table=check['liquibase_changeloglock_table'],
                warning=check['liquibase_lock_seconds_warning'],
                critical=check['liquibase_lock_seconds_critical']
            )

        if check['check_definer']:
            self.check_definer(targets=check['definer_targets'])

        if self._state == MySQLServer.state_critical:
            self._print_status('critical')

        elif self._state == MySQLServer.state_warning:
            self._print_status('warning')

        else:
            self._print_status('ok')

        return self._state


def parse_cmd_args():
    """
    parses command line arguments

    @returns: parsed arguments
    @returntype: argparse.Namespace
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', required=True)
    parser.add_argument('-U', '--user')
    parser.add_argument('-p','--passwd')
    parser.add_argument(
        '--defaults-file',
        dest='read_default_file',
        default='~/.my.cnf',
        help='Full path to my.cnf (default: ~/.my.cnf)'
    )

    parser.add_argument(
        '--db',
        default='information_schema',
        help='Database connect to (default: information_schema)'
    )
    parser.add_argument(
        '-P', '--port',
        type=int,
        default=3306,
        help='Database Port (default: 3306)'
    )
    parser.add_argument(
        '--connect-timeout',
        type=int,
        default=5,
        help='Database connection timeout in seconds (default: 5s)'
    )
    parser.add_argument(
        '--ssl-key',
        help='Path to ssl client private key file'
    )
    parser.add_argument(
        '--ssl-cert',
        help='Path to ssl client public key certificate file'
    )
    parser.add_argument(
        '--ssl-ca',
        help='Path to ssl CA certificate file'
    )
    parser.add_argument(
        '--ssl-capath',
        help='Path to directory with trusted ssl CA certificates file'
    )

    group_threads = parser.add_argument_group('Thread usage check')
    group_threads.add_argument(
        '--check-threads',
        action='store_true',
        help='Enable thread check'
    )
    group_threads.add_argument(
        '--threads-warning',
        default=60,
        type=int,
        help='Warning threshold in percentage for concurrency thread usage (default: 60)'
    )
    group_threads.add_argument(
        '--threads-critical',
        default=95,
        type=int,
        help='Critical threshold in percentage for concurrency thread usage (default: 95)'
    )

    group_con = parser.add_argument_group('Connection check')
    group_con.add_argument(
        '--check-connections',
        action='store_true',
        help='Enables connection check'
    )
    group_con.add_argument(
        '--connections-warning',
        default=85,
        type=int,
        help='Warning threshold in percentage for connection usage (default: 85)'
    )
    group_con.add_argument(
        '--connections-critical',
        default=95,
        help='Critical threshold in percentage for connection usage (default: 95)'
    )

    group_slave = parser.add_argument_group('Slave connection check')
    group_slave.add_argument(
        '--check-slave-connections',
        action='store_true',
        help='Enables slave connection check'
    )
    group_slave.add_argument(
        '--slave-connections-warning',
        default=-1,
        type=int,
        help='Warning count of connected slave hosts (default: -1)'
    )
    group_slave.add_argument(
        '--slave-connections-critical',
        default=0,
        type=int,
        help='critical count of connected slave hosts (default: 0)'
    )

    group_repl = parser.add_argument_group('Replication delay check')
    group_repl.add_argument(
        '--check-replication',
        action='store_true',
        help='Enable replication check'
    )
    group_repl.add_argument(
        '--replication-ignore-readonly-warning',
        action='store_true',
        help='Ignore warning messages'\
             'when slave is not running in readonly mode'
    )
    group_repl.add_argument(
        '--replication-lag-seconds-warning',
        default=600,
        type=int,
        help='Warning threshold '\
             'in seconds for replication (default: 600)'
    )
    group_repl.add_argument(
        '--replication-lag-seconds-critical',
        default=1800,
        type=int,
        help='Critical threshold '\
             'in seconds for replication (default: 1800)'
    )
    group_repl.add_argument(
        '--replication-lag-bytes-warning',
        default=52428800,
        type=int,
        help='Warning threshold '\
             'in bytes for replication (default: 52428800)'
    )
    group_repl.add_argument(
        '--replication-lag-bytes-critical',
        default=104857600,
        type=int,
        help='Critical threshold '\
             'in bytes for replication (default: 104857600)'
    )

    group_conn = parser.add_argument_group('User connection check')
    group_conn.add_argument(
        '--check-user-connections',
        action='store_true',
        help='Enable user connection check'
    )
    group_conn.add_argument(
        '--user-connections-warning',
        default=20,
        type=int,
        help='Warning and critical alert '\
             'for user connections equal or below value '\
             '(default: 20)'
    )
    group_conn.add_argument(
        '--user-connections-critical',
        default=5,
        type=int,
        help='Critical threshold '\
             'for user connections equal or below value '\
             '(default: 5)'
    )
    group_conn.add_argument(
        '--user-connections-max-alertlevel',
        default='warning',
        choices=['warning','critical'],
        help='Define max alert level for user connections check (default: warning)'
    )
    group_conn.add_argument(
        '--user-connections-filter', default='root',
        help='Filter connections by username (default: root)'
    )

    group_liquibase = parser.add_argument_group('Liquibase check')
    group_liquibase.add_argument(
        '--check-liquibase',
        action='store_true',
        help='Enable liquibase check'
    )
    group_liquibase.add_argument(
        '--liquibase-lock-seconds-warning',
        default=900,
        type=int,
        help='Warning threshold for '\
             'lock held in seconds (default: 900)'
    )
    group_liquibase.add_argument(
        '--liquibase-lock-seconds-critical',
        default=3600,
        type=int,
        help='Critical threshold for '\
             'lock held in seconds (default: 3600)'
    )
    group_liquibase.add_argument(
        '--liquibase-database',
        help='database to check for databasechangeloglock table (default: all)'
    )
    group_liquibase.add_argument(
        '--liquibase-changeloglock-table',
        default='DATABASECHANGELOGLOCK',
        help='Table name for changeloglock table (default: DATABASECHANGELOGLOCK)'
    )

    group_heartbeat = parser.add_argument_group('Heartbeat check')
    group_heartbeat.add_argument(
        '--check-heartbeat',
        action='store_true',
        help='Enable heartbeat check'
    )
    group_heartbeat.add_argument(
        '--heartbeat-table',
        default='heartbeat.heartbeat',
        help='Name of the heartbeat table full qualified (default: heartbeat.heartbeat)'
    )
    group_heartbeat.add_argument(
        '--heartbeat-column',
        default='tz',
        help='Name of the heartbeat column to update'
    )

    group_definer = parser.add_argument_group('Definer check')
    group_definer.add_argument(
        '--check-definer',
        action='store_true',
        help='Enable definer check'
    )
    group_definer.add_argument(
        '--definer-targets',
        type=str,
        nargs='*',
        default=['views','routines','triggers','events'],
        choices=['views','routines','triggers','events'],
        help='Check for none existing definers'
    )

    args = parser.parse_args()

    return args


def parse_check_args(args):
    """
    validates threshold parameters and store options in data dict
    check format for warning and critical values are correct

    @param args: commandline arguments
    @type: argparse.Namespace

    @returns: dict with critical and warning value for each check type
    @returntype: dict
    """

    data = {}

    data['check_threads'] = args.check_threads
    data['threads_warning'] = args.threads_warning
    data['threads_critical'] = args.threads_critical

    data['check_slave_connections'] = args.check_slave_connections
    data['slave_connections_warning'] = args.slave_connections_warning
    data['slave_connections_critical'] = args.slave_connections_critical

    data['check_connections'] = args.check_connections
    data['connections_warning'] = args.connections_warning
    data['connections_critical'] = args.connections_critical

    data['check_user_connections'] = args.check_user_connections
    data['user_connections_warning'] = args.user_connections_warning
    data['user_connections_critical'] = args.user_connections_critical
    data['user_connections_filter'] = args.user_connections_filter
    data['user_connections_max_alertlevel'] = args.user_connections_max_alertlevel

    data['check_replication'] = args.check_replication
    data['replication_ignore_readonly_warning'] = args.replication_ignore_readonly_warning
    data['replication_lag_seconds_warning'] = args.replication_lag_seconds_warning
    data['replication_lag_seconds_critical'] = args.replication_lag_seconds_critical
    data['replication_lag_bytes_warning'] = args.replication_lag_bytes_warning
    data['replication_lag_bytes_critical'] = args.replication_lag_bytes_critical

    data['check_liquibase'] = args.check_liquibase
    data['liquibase_lock_seconds_warning'] = args.liquibase_lock_seconds_warning
    data['liquibase_lock_seconds_critical'] = args.liquibase_lock_seconds_critical
    data['liquibase_database'] = args.liquibase_database
    data['liquibase_changeloglock_table'] = args.liquibase_changeloglock_table

    data['check_heartbeat'] = args.check_heartbeat
    data['heartbeat_table'] = args.heartbeat_table
    data['heartbeat_column'] = args.heartbeat_column

    data['check_definer'] = args.check_definer
    data['definer_targets'] = args.definer_targets

    return data


def parse_connection_args(args):
    """
    extract all mysql connection params from args
    which don't have None values set and returns those

    @param args: commandline arguments
    @type: argparse.Namespace

    @returns: dict with mysql connection params
    @returntype: dict
    """

    valid_ssl_params = [
                        'ssl_key',
                        'ssl_cert',
                        'ssl_ca',
                        'ssl_capath'
                       ]

    valid_connection_params = [
                               'host',
                               'user',
                               'passwd',
                               'read_default_file',
                               'db',
                               'port',
                               'connect_timeout'
                              ]
    valid_connection_params += valid_ssl_params

    connection_args = {}
    ssl = {}

    for arg in vars(args):
        if arg in valid_connection_params:
            value = getattr(args, arg)

            if value:
                if arg in valid_ssl_params:
                    ssl.update({arg.lstrip('ssl_'):value})

                else:
                    connection_args.update({arg:value})

    if len(ssl) > 0:
      connection_args.update({'ssl':ssl})

    return connection_args


def main():

    args = parse_cmd_args()

    try:
        server = MySQLServer(parse_connection_args(args))
        sys.exit(server.status(parse_check_args(args)))

    except MySQLServerConnectException, e:
        msg = "Database Connection failed"
        mysql_err_code = e.args[0][0]

        if mysql_err_code in (MYSQL_HOST_NOT_ALLOWED, MYSQL_ACCESS_DENIED):
            msg = "User is not allowed to connect"
            print("Warning - {}".format(msg))
            sys.exit(MySQLServer.state_warning)

        elif mysql_err_code == MYSQL_UNKOWN_HOST:
            msg = "No service is listening on {}:{}".format(args.host,
                                                            args.port)
            print("Unknown - {}".format(msg))
            sys.exit(MySQLServer.state_unknown)

        elif mysql_err_code == MYSQL_REPLICATION_SLAVE_PRIV:
            msg = "User has unsufficient privileges (REPLICATION SLAVE)"
            print(msg)
            sys.exit(MySQLServer.state_warning)

        else:
            print("Critical - {}".format(msg))
            sys.exit(MySQLServer.state_critical)


if __name__ == '__main__':
    main()
