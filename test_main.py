import unittest
import parser_data_manager
import ijson
import hashlib
import ijson
import sqlite3
from yoyo import read_migrations,get_backend
import collections


class main(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.backend = get_backend("sqlite:///test.db")
        migrations = read_migrations("./migrations")
        with cls.backend.lock():
            cls.backend.apply_migrations(cls.backend.to_apply(migrations))
        cls.cnx = sqlite3.connect("test.db")
        cls.cur = cls.cnx.cursor()
        cls.count = 0 

        def false_insert_val(self):
            self.cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
            self.cnx.commit()


    def insert_val(self, obj):
        hashed1 = self.hash_val(obj)
        if self.compare(hashed1) == None:
            obj = collections.OrderedDict(sorted(obj.items()))
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
                    hashed1
            )
        )
        self.count = (self.count + 1) % 10000
        if self.count == 0:
            self.cnx.commit()

            
    
    def hash_val(self,val):
        return hashlib.md5(str(val).encode("utf-8")).hexdigest()


    def compare(self,hash1):
        tab=self.cur.execute("""SELECT row_hash FROM my_table WHERE row_hash=?""",[hash1])
        for i in tab:
            return len(i)

    def setUp(self):
        self.cur.execute("""DELETE FROM my_table""") 

    
    def test_hash(self):
        
        self.assertTrue ("5eb63bbbe01eeed093cb22bb8f5acdc3" == self.hash_val("hello world"))
    def test_insert(self):
        
        obj = {"time": "2020-10-27T14:45:42+00:00", "remote_addr": "103.42.20.221", "remote_user": "03039", "body_bytes_sent": "162", "request_time": "0.000", "status": "301", "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1", "request_method": "POST", "http_referrer": "-", "http_user_agent": "SQLAnywhere/16.0.0.2546", "proxy_host": "-" }
        self.insert_val(obj)
        self.cur.execute("""SELECT time,
                    remote_addr,
                    remote_user,
                    body_bytes_sent,
                    request_time,
                    status,
                    request,
                    request_method,
                    http_referrer,
                    http_user_agent,
                    proxy_host FROM my_table """)
        row = self.cur.fetchone()
        self.assertTrue(row==tuple(obj.values()))

    def test_double_insert(self): 
        obj = {"time": "2021-10-27T14:45:42+00:00", "remote_addr": "103.42.20.221", "remote_user": "03039", "body_bytes_sent": "162", "request_time": "0.000", "status": "301", "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1", "request_method": "POST", "http_referrer": "-", "http_user_agent": "SQLAnywhere/16.0.0.2546", "proxy_host": "-" }
        for i in range(2):
            self.insert_val(obj)
        self.cur.execute("""SELECT * FROM my_table""")
        row = self.cur.fetchall()
        self.assertTrue(len(row) == 1)
        
        
        

        
if __name__ == "__main__":
      unittest.main()


