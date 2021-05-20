#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import hashlib
import collections


class Parser_data_manager:
    db_prefix = "sqlite:///"

    def __init__(self, connection_string):
        self.db_name = connection_string[
            connection_string.
            startswith(Parser_data_manager.db_prefix) and len(Parser_data_manager.db_prefix):]
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

    def false_insert_val(self, false_obj):
        false_obj = str(false_obj) + str(self._error_count)
        hashed_val = hashlib.md5(str(false_obj).encode("utf-8")).hexdigest()
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
                hashed_val,
                obj['file_name']))
        self._error_count += 1

    def insert_val(self, obj, file_name):
        obj = collections.OrderedDict(sorted(obj.items()))
        obj['file_name'] = file_name
        obj['time'] = obj['time'][:10] + ' ' + obj['time'][11:19]
        hashed_val = hashlib.md5(str(obj).encode("utf-8")).hexdigest()
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
            hashed_val,
            obj['file_name']))
        self.count = (self.count + 1) % 25000
        if self.count == 0:
            self._cnx.commit()

    def fetch_request_time_by_fname(self, func=None):
        self._cur.execute('SELECT request_time FROM my_table WHERE request LIKE ? GROUP BY request', ([func]))
        return self._cur.fetchall()

    def fetch_request_time_status_by_time(self, first_time, second_time):
        self._cur.execute("""SELECT request, status FROM my_table request
        WHERE (time >= :fromDateTime OR :fromDateTime is null)
        AND (time <= :toDateTime OR :toDateTime is null)
        GROUP BY request""", {'fromDateTime': first_time, 'toDateTime': second_time})
        return self._cur.fetchall()

    def fetch_requests(self, first_time, second_time):
        self._cur.execute("""SELECT time, request, remote_addr, status FROM my_table
        WHERE (time >= :fromDateTime OR :fromDateTime is null)
        AND (time <= :toDateTime OR :toDateTime is null)
        GROUP BY request""", {'fromDateTime': first_time, 'toDateTime': second_time})
        return self._cur.fetchall()
