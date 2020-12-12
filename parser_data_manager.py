import sqlite3
import ijson 


class Parser_data_manager:
    def __init__(self, connection_string):
        self._cnx = sqlite3.connect(connection_string)
        self._cur = self._cnx.cursor()
        self._count = 0   


    def false_insert(self):
        self._cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
        self._cnx.commit()


    def inserting(self, obj):
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
            proxy_host) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",(
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
                obj['proxy_host']
            )
        )
        self._count = (self._count + 1) % 100000
        if self._count == 0:
            self._cnx.commit()
        