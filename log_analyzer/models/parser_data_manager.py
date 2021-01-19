#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import hashlib
import collections
import numpy


class Parser_data_manager:
    def __init__(self, connection_string, file_name):
        try:
            self._cnx = sqlite3.connect(connection_string)
            self._cur = self._cnx.cursor()
            self._count = 0
            self._error_count = 1
            self.file_name = file_name
        except sqlite3.Error:
            print("Error connecting to database!")

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

    def insert_val(self, obj):
        obj = collections.OrderedDict(sorted(obj.items()))
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
            self.file_name))
        self._count = (self._count + 1) % 100000
        if self._count == 0:
            self._cnx.commit()

    @staticmethod
    def hash_val(val):
        return hashlib.md5(str(val).encode("utf-8")).hexdigest()

    def report(self, percent):
        mass = self._cur.execute('SELECT request_time FROM my_table')
        rep = int(numpy.percentile(mass, percent))
        return rep
