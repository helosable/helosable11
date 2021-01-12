#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import hashlib
import collections


class Parser_data_manager:
    def __init__(self, connection_string):
        try:
            self._cnx = sqlite3.connect(connection_string)
            self._cur = self._cnx.cursor()
            self._count = 0
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

    def false_insert_val(self):
        obj = {"time": "не получилось",
               "remote_addr": "не получилось",
               "remote_user": "не получилось",
               "body_bytes_sent": "не получилось",
               "request_time": "не получилось",
               "status": "не получилось",
               "request": "не получилось",
               "request_method": "не получилось",
               "http_referrer": "не получилось",
               "http_user_agent": "не получилось",
               "proxy_host": "не получилось"}
        with self._cnx as con:
            con.execute("""INSERT INTO my_table (
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
                row_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
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
                "не получилось"))

    def insert_val(self, obj):
        hashed1 = self.hash_val(obj)
        if self._compare(hashed1) is False:
            obj = collections.OrderedDict(sorted(obj.items()))
            self._cur.execute("""INSERT INTO my_table (
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
                row_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
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
                hashed1))
        self._count = (self._count + 1) % 1
        if self._count == 0:
            self._cnx.commit()

    @staticmethod
    def hash_val(val):
        return hashlib.md5(str(val).encode("utf-8")).hexdigest()

    def _compare(self, hash1):
        self._cur.execute("""SELECT row_hash FROM my_table WHERE row_hash=?""", [hash1])
        tab = self._cur.fetchall()
        return len(tab) > 0
