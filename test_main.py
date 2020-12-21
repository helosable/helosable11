import unittest
import parser_data_manager
import ijson
import hashlib
import ijson
import sqlite3


dm=parser_data_manager.Parser_data_manager("main.db")

class main(unittest.TestCase):
    
    def test_hash(self):
        self.assertTrue ("5eb63bbbe01eeed093cb22bb8f5acdc3"==parser_data_manager.Parser_data_manager.hash_val("hello world"))
    def test_insert(self):
        obj={"time": "2020-10-27T14:45:42+00:00", "remote_addr": "103.42.20.221", "remote_user": "03039", "body_bytes_sent": "162", "request_time": "0.000", "status": "301", "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1", "request_method": "POST", "http_referrer": "-", "http_user_agent": "SQLAnywhere/16.0.0.2546", "proxy_host": "-" }
        dm.insert_val(obj)
        dm._cur.execute("""SELECT * FROM my_table """)
        row=dm._cur.fetchone()
        lst=[]
        lst1=[]
        loop=0
        for val in obj.values():
            lst.append(val)
        for  val in row:
            if loop==0 or loop == 12:
                loop+=1
                continue
            lst1.append(val)
            loop+=1
        self.assertTrue(lst1==lst)

if __name__ == "__main__":
      unittest.main()


