import unittest
import sqlite3
from yoyo import read_migrations, get_backend
from log_analyzer.models.parser_data_manager import Parser_data_manager as pdm


class main(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///test.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))
                    

    def setUp(self):
        with sqlite3.connect('test.db') as cnx:
            cnx.execute("""DELETE FROM my_table""")

    def test_hash(self):
        self.assertTrue("5eb63bbbe01eeed093cb22bb8f5acdc3" == pdm.hash_val("hello world"))

    def test_insert(self):
        obj = {"time": "2020-10-27T14:45:42+00:00",
               "remote_addr": "103.42.20.221",
               "remote_user": "03039",
               "body_bytes_sent": "162",
               "request_time": "0.000",
               "status": "301",
               "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1",
               "request_method": "POST",
               "http_referrer": "-",
               "http_user_agent": "SQLAnywhere/16.0.0.2546",
               "proxy_host": "-"}
        with pdm('test.db') as dm:
            dm.insert_val(obj)
        with sqlite3.connect('test.db') as cnx:
            cur = cnx.cursor()
            cur.execute("""SELECT time,
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
        row = cur.fetchone()
        self.assertTrue(row == tuple(obj.values()))

    def test_double_insert(self):
        obj = {"time": "2021-10-27T14:45:42+00:00",
               "remote_addr": "103.42.20.221",
               "remote_user": "03039",
               "body_bytes_sent": "162",
               "request_time": "0.000",
               "status": "301",
               "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1",
               "request_method": "POST",
               "http_referrer": "-",
               "http_user_agent": "SQLAnywhere/16.0.0.2546",
               "proxy_host": "-"}
        with pdm('test.db') as dm:
            for i in range(2):
                dm.insert_val(obj)
        with sqlite3.connect('test.db') as cnx:
            cur = cnx.cursor()
            cur.execute("""SELECT * FROM my_table""")
            row = cur.fetchall()
            self.assertTrue(len(row) == 1)

if __name__ == "__main__":
    unittest.main()
