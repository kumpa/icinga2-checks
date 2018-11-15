#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Patrick Kummutat"
__version__ = "0.2"
__date__ = "15/11/2018"

import argparse
import sys
import MySQLdb


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

        self._state = MySQLServer.state_ok
        self._messages = dict(ok=list(),
                              warning=list(),
                              critical=list())

        self._perf_data = list()
        self._mysql = dict()
        self._connection = None
        self._is_slave = False

        try:
            self._connection = MySQLdb.connect(**kwargs)
            self._cursor = self._connection.cursor(
                    cursorclass=MySQLdb.cursors.DictCursor)
        
        except Exception, e:
            raise MySQLServerConnectException

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


    def _run_query(self, sql, fetch='all'):
        """
        @param sql: any valid sql to execute
        @type: string
        @param fetch: returns all fetched rows or
                      just one line
        @type: string
        """

        self._cursor.execute(sql)

        if fetch == 'one':
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


    def check_threads(self, warning=60, critical=95):
        """
        calculate thread usage in percentage

        @param warning: threshold in percentage
        @type warning: int or float
        @param critical: threshold in percentage
        @type warning: int or float
        """
        threads_running = float(self._mysql['status'].get('Threads_running', 0))
        thread_concurrency = float(self._mysql['variables'].get('innodb_thread_concurrency'))
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
        

    def check_connections(self, warning=85, critical=95):
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
        

    def check_replication(self, warning=600, critical=1800):
        """
        examine the replication status of a slave

        @param warning: threshold in seconds
        @type warning: int or float
        @param critical: threshold in seconds
        @type critical: int or float
        """

        if self._is_slave:
            read_only = self._mysql['variables'].get('read_only')
            seconds_behind = self._mysql['slave'].get('Seconds_Behind_Master', 0)
            large_lag = 999999
            read_master_pos = self._mysql['slave'].get('Read_Master_Log_Pos', 0)
            exec_master_pos = self._mysql['slave'].get('Exec_Master_Log_Pos', 0)
            slave_sql = self._mysql['slave'].get('Slave_SQL_Running', 'No')
            slave_io = self._mysql['slave'].get('Slave_IO_Running', 'No')
            slave_err = self._mysql['slave'].get('Last_Errno', '')
            master_host = self._mysql['slave'].get('Master_Host')
            master_port = self._mysql['slave'].get('Master_Port')

            lag_seconds = int(seconds_behind if seconds_behind >= 0 else large_lag)
            lag_bytes = read_master_pos - exec_master_pos
            self._perf_data.append("replicaton_seconds={}s;{};{}".format(seconds_behind,
                                                                         warning,
                                                                         critical))

            if slave_sql != 'Yes':
                msg = "Replication SQL Thread is down"
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

                if slave_err:
                    msg = "Last Error: {}".format(slave_err)
                    self._messages['critical'].append(msg)

            if slave_io != 'Yes':
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

            if lag_seconds >= critical:
                self._messages['critical'].append(msg)
                self._set_state(MySQLServer.state_critical)

            elif lag_seconds >= warning:
                self._messages['warning'].append(msg)
                self._set_state(MySQLServer.state_warning)

            else:
                self._messages['ok'].append(msg)


    def status(self):
        """
        calls all check functions and generate
        the status output

        @returns: the exit code for icinga
        @returntype: int
        """

        self.check_threads()
        self.check_connections()
        self.check_replication()
        
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
    parser.add_argument('--read-default-file', default='~/.my.cnf')
    parser.add_argument('--db', default='mysql')
    parser.add_argument('-P', '--port', type=int, default=3306)
    args = parser.parse_args()

    return args


def main():

    args = parse_cmd_args()
    db_params = dict(filter(lambda item: item[1] is not None,
                            vars(args).items()))
    
    try:
        server = MySQLServer(db_params)
        sys.exit(server.status())

    except MySQLServerConnectException:
        msg = "Database Connection failed"
        print("Critical {}".format(msg))
        sys.exit(MySQLServer.state_critical)


if __name__ == '__main__':
    main()
