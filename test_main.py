import unittest
import parser_data_manager
import ijson
import hashlib
import ijson
import sqlite3

dm=parser_data_manager.Parser_data_manager("test_db.db")

class main(unittest.TestCase):
    def __init__(self):
        self.json1='{ "time": "2020-10-27T14:45:42+00:00", "remote_addr": "103.42.20.221", "remote_user": "03039", "body_bytes_sent": "162", "request_time": "0.000", "status": "301", "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1", "request_method": "POST", "http_referrer": "-", "http_user_agent": "SQLAnywhere/16.0.0.2546", "proxy_host": "-" }'
        self.obj=next(ijson.items(self.json1))
               

    def test_hash(self):
        self.assertTrue ("5eb63bbbe01eeed093cb22bb8f5acdc3"==parser_data_manager.Parser_data_manager.hash_val("hello world"))
    def test_insert(self):
        dm.insert_val(self.obj)
        self.assertTrue(self.cur.execute("""SELECT * FROM my_table """)==self.obj)
        
        
       
        

if __name__ == "__main__":
      unittest.main()


