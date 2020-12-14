import sqlite3
import ijson 
import hashlib
import collections


class Parser_data_manager:
    def __init__(self, connection_string):
        self._cnx = sqlite3.connect(connection_string)
        self._cur = self._cnx.cursor()
        self._count = 0 


    def false_insert_val(self):
        self._cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
        self._cnx.commit()


    def insert_val(self, obj):
        if self.compare(self.hash_val(obj))==None:
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
                row_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",(
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
                    self.hash_val(obj)
            )
        )
        self._count = (self._count + 1) % 10000
        if self._count == 0:
            self._cnx.commit()

    def hash_val(self,val):
        self._pre_hashed_value=str(val)
        self._pre_hashed_value = self._pre_hashed_value.encode('utf-8')
        self._hashed_value = hashlib.md5(self._pre_hashed_value)
        return self._hashed_value.hexdigest()


    def compare(self,hash):
        tab=self._cur.execute("""SELECT row_hash FROM my_table WHERE row_hash=?""",[f'{hash}'])
        for i in tab:
            return len(i)
            

    
        
