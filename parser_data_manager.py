import sqlite3
import ijson 


class Parser_data_manager:
    def __init__(self, connection_string):
        self.cnx = sqlite3.connect(connection_string)
        self.cur = self.cnx.cursor()   


    def false_insert(self):
        self.cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
        self.cnx.commit()


    def inserting(self, obj):
        self.cur.execute("""INSERT INTO my_table (
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
        self.cnx.commit()
        