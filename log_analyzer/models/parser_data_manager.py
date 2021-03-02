#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import hashlib
import collections
import numpy

class Parser_data_manager:
    def __init__(self, connection_string, first_time='2020-10-27 14:45:42', second_time='2020-10-27 14:45:43'):
        self.first_time = first_time
        self.second_time = second_time
        try:
            self._cnx = sqlite3.connect(connection_string)
            self._cur = self._cnx.cursor()
            self._count = 0
            self._error_count = 1
        except sqlite3.Error:
            print("Error connecting to database!")

    def second_migration(self):
        self._cur.execute("SELECT * FROM my_table")
        p = self._cur.fetchall()
        try:
            self._cur.execute("""CREATE TABLE new_table (id INTEGER AUTO_INCREMENT,
        time date,
        remote_addr VARCHAR,
        remote_user VARCHAR,
        body_bytes_sent VARCHAR,
        request_time VARCHAR,
        status VARCHAR,
        request VARCHAR,
        request_method VARCHAR,
        http_referrer VARCHAR,
        http_user_agent VARCHAR,
        proxy_host VARCHAR,
        row_hash VARCHAR(35),
        file_name VARCHAR,
        PRIMARY KEY(id))""")
        except sqlite3.OperationalError:
            pass
        try:
            self._cur.execute("CREATE UNIQUE INDEX hash_unique_index_1 ON new_table(row_hash)")
        except sqlite3.OperationalError:
            pass
        for i in p:
            obj = {'time': f'{i[0]}', 'remote_addr': f'{i[1]}', 'remote_user': f'{i[2]}',
                'body_bytes_sent': f'{i[3]}',
                'request_time': f'{i[4]}', 'status': f'{i[5]}', 'request' : f'{i[6]}', 
                'request_method': f'{i[7]}', 'http_referrer': f'{i[8]}',
                'http_user_agent': f'{i[9]}', 'proxy_host': f'{i[10]}', 
                'file_name': f'{i[12]}'}
            if len(obj['time']) > 20:
                self.insert_val(obj, obj['file_name'])
        try:
            self._cur.execute("DROP TABLE my_table")
        except sqlite3.OperationalError :
            pass
        try:
            self._cur.execute("ALTER TABLE new_table RENAME TO my_table")
        except sqlite3.OperationalError :
            pass

    def close(self):
        if self._cnx:
            self._cnx.commit()
            self._cur.close()
            self._cnx.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

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
        self._count = (self._count + 1) % 100000
        if self._count == 0:
            self._cnx.commit()

    def per_report(self):
        first_time = self.first_time
        second_time = self.second_time
        time_first = f"{first_time}"
        time_second = f"{second_time}"
        per_list = [50, 75, 95, 99]
        time_list = []
        c_1 = 0
        self._cur.execute(f"SELECT request FROM my_table WHERE time BETWEEN ? AND ? GROUP BY request ",(self.first_time, self.second_time))
        func_name = list(self._cur.fetchall())
        time_list.append(['func_name', '50 per', '90 per', '95 per', '99 per'])
        for func in func_name:
            func = func[0]
            new_func = ''
            if func == 'error':
                continue
            self._cur.execute(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
            time = self._cur.fetchall()
            new_time = []
            new_per_list = []
            q = 1
            for i in func:
                for l in i :
                    if q == 0:
                        break
                    if l == '?' or l == ';':
                        l = ''
                        q = 0
                    else:
                        new_func += l
            new_per_list.append(new_func)
            for i in time:
                new_time.append(float(i[0]))
            for i in per_list:
                i1 = float(numpy.percentile(new_time, i))
                if len(f'{i1}') > 6 :
                    i1 = float('{:.5f}'.format(i1))
                new_per_list.append(i1)   
            time_list.append(new_per_list)
        return time_list

    def ip_report(self):
        rep_list = []
        rep_list.append(['time', 'func', 'ip'])
        self._cur.execute("SELECT time, request, remote_addr FROM my_table WHERE time BETWEEN ? AND ?",(self.first_time, self.second_time))
        ip_name = list(self._cur.fetchall())
        for ip in ip_name:
            func = ip[1]
            new_func = ''
            if ip == 'error':
                continue
            q = 1
            for i in func:
                for l in i :
                    if q == 0:
                        break
                    if l == '?' or l == ';':
                        l = ''
                        q = 0
                    else:
                        new_func += l
                    time_list = [ip[0], new_func, ip[2]]
            rep_list.append(time_list)
        return rep_list

    @staticmethod
    def hash_val(val):
        return hashlib.md5(str(val).encode("utf-8")).hexdigest()

