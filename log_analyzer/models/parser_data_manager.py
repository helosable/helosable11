#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import hashlib
import collections
import numpy
from yoyo import read_migrations, get_backend


class Parser_data_manager:
    def __init__(self, connection_string):
        self.db_name = connection_string
        self._cnx = sqlite3.connect(self.db_name)
        self._cur = self._cnx.cursor()
        self.count = 0
        self._error_count = 1

    def close(self):
        if self._cnx:
            self._cnx.commit()
            self._cur.close()
            self._cnx.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def migrate(self):
        backend = get_backend(f"sqlite:///{self.db_name}")
        migration = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migration))

    def false_insert_val(self, false_obj):
        false_obj = str(false_obj) + str(self._error_count)
        hashed1 = self.hash_val(false_obj)
        obj = {"time": "error",
               "remote_addr": "error",
               "remote_user": "error",
               "body_bytes_sent": "error",
               "request_time": "error",
               "status": "error",
               "request": "error",
               "request_method": "error",
               "http_referrer": "error",
               "http_user_agent": "error",
               "proxy_host": "error",
               "file_name": "error"}
        with self._cnx as con:
            con.execute("""INSERT OR IGNORE INTO my_table (
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
                file_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                obj['time'],
                obj['remote_addr'],
                obj['remote_user'],
                obj['body_bytes_sent'],
                obj['request_time'],
                obj['status'],
                obj['request'],
                obj['request_method'],
                obj['http_referrer'],
                obj['http_user_agent'],
                obj['proxy_host'],
                hashed1,
                obj['file_name']))
        self._error_count += 1

    def insert_val(self, obj, file_name):
        obj = collections.OrderedDict(sorted(obj.items()))
        obj['file_name'] = file_name
        obj['time'] = obj['time'][:10] + ' ' + obj['time'][11:19]
        hashed1 = self.hash_val(obj)
        self._cur.execute("""INSERT OR IGNORE INTO my_table (
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
            file_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            obj['time'],
            obj['remote_addr'],
            obj['remote_user'],
            obj['body_bytes_sent'],
            obj['request_time'],
            obj['status'],
            obj['request'],
            obj['request_method'],
            obj['http_referrer'],
            obj['http_user_agent'],
            obj['proxy_host'],
            hashed1,
            obj['file_name']))
        self.count = (self.count + 1) % 250000
        if self.count == 0:
            self._cnx.commit()

    def func_name(self, func):
        new_func = ''
        for i in func:
            q = 1
            for l1 in i:
                if q == 0:
                    break
                if l1 == '?' or l1 == ';':
                    l1 = ''
                    q = 0
                else:
                    new_func += l1
        return new_func

    def per_report(self, first_time, second_time):
        per_list = [50, 75, 95, 99]
        time_list = []
        self._cur.execute("""SELECT request FROM my_table
        WHERE time BETWEEN ? AND ? GROUP BY request""", (first_time, second_time))
        func_name = self._cur.fetchall()
        time_list.append(['func_name', '50 per', '90 per', '95 per', '99 per'])
        for func in func_name:
            func = func[0]
            if func == 'error':
                continue
            self._cur.execute(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
            time = self._cur.fetchall()
            new_time = []
            new_per_list = []
            new_per_list.append(self.func_name(func))
            for i in time:
                new_time.append(float(i[0]))
            for i in per_list:
                i1 = float(numpy.percentile(new_time, i))
                if len(f'{i1}') > 6:
                    i1 = float('{:.5f}'.format(i1))
                new_per_list.append(i1)
            time_list.append(new_per_list)
        return time_list

    def ip_report(self, first_time, second_time):
        rep_list = []
        rep_list.append(['time', 'func', 'ip'])
        self._cur.execute("""SELECT time, request, remote_addr
        FROM my_table WHERE time BETWEEN ? AND ? GROUP BY request""", (first_time, second_time))
        ip_name = self._cur.fetchall()
        for ip in ip_name:
            if ip == 'error':
                continue
            time_list = [ip[0], self.func_name(ip[1]), ip[2]]
            rep_list.append(time_list)
        return rep_list

    @staticmethod
    def hash_val(val):
        return hashlib.md5(str(val).encode("utf-8")).hexdigest()
