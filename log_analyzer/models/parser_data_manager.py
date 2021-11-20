#!/usr/bin/env python
# -*- coding: utf-8 -*-


from clickhouse_driver import connect
import hashlib
import collections


class Parser_data_manager:

    def __init__(self, connection_string, table_name):
        self._table_name = table_name
        self.db_name = connection_string
        self._connection = connect(connection_string)
        self._cnx = self._connection.cursor()
        self.count = 0
        self._error_count = 1

    def close(self):
        if self._cnx:
            self._cnx.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def migrate(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../migrations")
        from m20201206_000_initial import sql_query
        self._cnx.execute(sql_query(self._table_name))

    def false_insert_val(self):
        self._cnx.execute(f"INSERT INTO {self._table_name}"+""" (
                time,
                remote_addr,
                remote_user,
                body_bytes_sent,
                request_time,
                status,
                request,
                request_method,
                http_referrer,
                http_user_agent,
                proxy_host,
                row_hash,
                file_name) """ +
            'VALUES ('+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error', "+
                "'error' )")

    def insert_val(self, obj, file_name):
        if type(obj) == bool:
            return 0
        obj = collections.OrderedDict(sorted(obj.items()))
        obj['time'] = obj['time'][:10] + ' ' + obj['time'][11:19]
        obj['row_hash'] = hashlib.md5(str(obj).encode("utf-8")).hexdigest()
        self._cnx.execute(f'select id from {self._table_name}')
        id = self._cnx.fetchall()
        # print(id)
        # print(len(id))
        # id = 1
        if len(id) == 0:
            id = 1
        else :
            id = int(id[-1][0] + 1)

        time = f"{obj['time']}"
        remote_addr = f"{obj['remote_addr']}"
        remote_user = f"{obj['remote_user']}"
        body_bytes_sent = f"{obj['body_bytes_sent']}"
        request_time = f"{obj['request_time']}"
        status = f"{obj['status']}"
        request = f"{obj['request']}"
        request_method = f"{obj['request_method']}"
        http_referrer = f"{obj['http_referrer']}"
        http_user_agent = f"{obj['http_user_agent']}"
        proxy_host = f"{obj['proxy_host']}"
        row_hash = f"{obj['row_hash']}"
        self._cnx.execute(f"select * from {self._table_name} where row_hash in '{row_hash}'")
        dupl_string = self._cnx.fetchall()
        dupl_string = len(dupl_string)
        if dupl_string != 0:
            return 0
        self._cnx.execute(
         f"INSERT INTO {self._table_name}"+""" (
                id,
                time,
                remote_addr,
                remote_user,
                body_bytes_sent,
                request_time,
                status,
                request,
                request_method,
                http_referrer,
                http_user_agent,
                proxy_host,
                row_hash,
                file_name)
            VALUES"""+f" ({id},'{time}', '{remote_addr}', '{remote_user}', '{body_bytes_sent}',"+ 
            f"'{request_time}', '{status}', '{request}', '{request_method}', '{http_referrer}', "+
            f"'{http_user_agent}', '{proxy_host}', '{row_hash}', '{file_name}') ")

    def fetch_request_time_by_fname(self, func=None):
        self._cnx.execute(
            f"SELECT request_time FROM {self._table_name} WHERE request LIKE '{func}'")
        return self._cnx.fetchall()

    def fetch_request_time_status_by_time(self, first_time, second_time):
        self._cnx.execute(f"SELECT request, status, remote_addr FROM {self._table_name} request "+
        f"WHERE time BETWEEN '{first_time}' AND '{second_time}'  GROUP BY request, status, remote_addr ")
        return self._cnx.fetchall()

    def fetch_requests(self, first_time, second_time):
        self._cnx.execute(f"SELECT time, request, remote_addr, status FROM {self._table_name} "+
        f"WHERE time BETWEEN toDateTime('{first_time}') AND toDateTime('{second_time}')  GROUP BY time, request, remote_addr, status")
        return self._cnx.fetchall()
