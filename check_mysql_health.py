#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Patrick Kummutat"
__version__ = "0.6"
__date__ = "15/11/2018"

import argparse
import sys
import MySQLdb


MYSQL_HOST_NOT_ALLOWED = 1130
MYSQL_UNKOWN_HOST = 2005
MYSQL_ACCESS_DENIED = 1045
MYSQL_REPLICATION_SLAVE_PRIV = 1227
MYSQL_RESULT_FETCH_ALL = "ALL"
MYSQL_RESULT_FETCH_ONE = "ONE"


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


    def check_users(self, warning, critical):
        """
        checks count of connected database users

        @param warning: threshold for warning
        @type warning: int or float
        @param critical: threshold for critical
        @type warning: int or float
        """

        if warning == -1 or critical == -1:
            return

        user = self._run_query("SELECT COUNT(*) AS connected_users " \
                               "FROM information_schema.processlist",
                               MYSQL_RESULT_FETCH_ONE)
        user_connected = user['connected_users']

        perf_data = "connected_users={}:{}:{}".format(user_connected,
                                                      warning,
                                                      critical)
        self._perf_data.append(perf_data)
        msg = "Connected users {}".format(user_connected)

        if user_connected >= critical:
            self._messages['critical'].append(msg)
            self._set_state(MySQLServer.state_critical)

        elif user_connected >= warning:
            self._messages['warning'].append(msg)
            self._set_state(MySQLServer.state_warning)

        else:
            self._messages['ok'].append(msg)


    def check_threads(self, warning, critical):
        """
        calculate thread usage in percentage

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
            

    def check_replication(self, threshold_seconds, threshold_bytes):
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
                                                   threshold_bytes.get('warning'),
                                                   threshold_bytes.get('critical')))

            if lag_seconds is None:
                lag_seconds = -1

            perf_msg = "replication_lag_seconds={}s;{};{}"
            self._perf_data.append(perf_msg.format(lag_seconds,
                                                   threshold_seconds.get('warning'),
                                                   threshold_seconds.get('critical')))

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

            if read_only != 'ON':
                msg = "Slave is not operating in read only mode"
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            msg = "Replication "\
                  "Master {}:{} "\
                  "Slave lag {}s/{}B".format(master_host,
                                             master_port,
                                             lag_seconds,
                                             lag_bytes)

            if lag_bytes >= threshold_bytes.get('critical'):
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            elif lag_bytes >= threshold_bytes.get('warning'):
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            else:
                pass
                   
            if lag_seconds >= threshold_seconds.get('critical'):
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            elif lag_seconds >= threshold_seconds.get('warning'):
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            else:
                self._messages['ok'].append(msg)


    def check_slave_count(self, warning, critical):
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


    def status(self, check):
        """
        calls all check functions and generate
        the status output
        
        @param check: threshold for each check command
        @returns: the exit code for icinga
        @returntype: int
        """

        if check['check_replication']:
            self.check_replication(check['replication_lag_seconds'],
                                   check['replication_lag_bytes'])

        self.check_threads(**check['check_threads'])

        if check['check_user_connections']:
            self.check_user_connections(username=check['user_connections_filter'],
                                        alert_level=check['user_connections_max_alertlevel'],
                                        **check['user_connections'])

        self.check_connections(**check['check_connections'])

        self.check_slave_count(**check['check_slave_count'])

        self.check_users(**check['check_users'])
        
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
    parser.add_argument('--defaults-file', 
                        dest='read_default_file', 
                        default='~/.my.cnf',
                        help='Full path to my.cnf'
                       )

    parser.add_argument('--db', default='mysql')
    parser.add_argument('-P', '--port', type=int, default=3306)

    group = parser.add_argument_group('check')
    group.add_argument('--check-users', default='-1:-1',
            help='warning and critical threshold '\
                 'for connected users (float|int:float|int)')

    group.add_argument('--check-threads', default='60:95',
            help='warning and critical threshold '\
                 'in percent for concurrency thread usage (float|int:float|int)')

    group.add_argument('--check-connections', default='85:95',
            help='warning and critical threshold '\
                 'in percent for connection usage (float|int:float|int)')

    group.add_argument('--check-slave-count', default='-1:-1',
            help='warning and critical count ' \
                 'of connected slave hosts  (float|int:float|int)')

    group_repl = parser.add_argument_group('replication check')
    group_repl.add_argument('--check-replication', action='store_true',
            help='enable replication check')

    group_repl.add_argument('--replication-lag-seconds', default='600:1800',
            help='warning and critical threshold '\
                 'in seconds for replication (float|int:float|int)')

    group_repl.add_argument('--replication-lag-bytes', default='52428800:104857600',
            help='warning and critical threshold '\
                 'in bytes for replication (float|int:float|int)')

    group_conn = parser.add_argument_group('user connection check')
    group_conn.add_argument('--check-user-connections', action='store_true',
            help='enable user connection check')

    group_conn.add_argument('--user-connections', default='20:5',
            help='warning and critical alert '\
                 'for user connections equal or below thresholds '\
                 '(default: warn=20, crit=5)')
    
    group_conn.add_argument('--user-connections-max-alertlevel', default='warning',
            choices=['warning','critical'],
            help='define max alert level for user connections check (default: warning)')

    group_conn.add_argument('--user-connections-filter', default='root',
            help='filter connections by username (default: root)')

    args = parser.parse_args()

    return args


def parse_threshold(args):
    """
    parses given thresholds for warning and critical,
    values are separated by colon

    @param args: warn and crit threshold
    @type: string

    @returns: warning and critical threshold
    @returntype: dict
    """

    threshold = args.split(':')

    if len(threshold) == 2:
       return dict(warning=float(threshold[0]),
                   critical=float(threshold[1]))

    else:
       print("Invalid threshold format. Use --check <warn>:<crit>")
       sys.exit(-1)


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
 
    data['check_threads'] = parse_threshold(args.check_threads)

    data['check_slave_count'] = parse_threshold(args.check_slave_count)
    data['check_users'] = parse_threshold(args.check_users)
    data['check_connections'] = parse_threshold(args.check_connections)

    data['check_user_connections'] = args.check_user_connections
    data['user_connections'] = parse_threshold(args.user_connections)
    data['user_connections_filter'] = args.user_connections_filter
    data['user_connections_max_alertlevel'] = args.user_connections_max_alertlevel

    data['check_replication'] = args.check_replication
    data['replication_lag_seconds'] = parse_threshold(args.replication_lag_seconds)
    data['replication_lag_bytes'] = parse_threshold(args.replication_lag_bytes)

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
 
    valid_connection_params = ['host',
                               'user',
                               'passwd',
                               'read_default_file',
                               'db',
                               'port']
    connection_args = {}

    for arg in vars(args):
        if arg in valid_connection_params:
           value = getattr(args, arg)
           if value:
              connection_args.update({arg:value})

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
