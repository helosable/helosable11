import unittest
from clickhouse_driver import connect
from log_analyzer.models.parser_data_manager import Parser_data_manager as pdm
from main import json_read

class main(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings = json_read('test_config.json')
        db_query = f"clickhouse://{settings['db_user_name']}:{settings['db_password']}@{settings['db_ip']}:{settings['db_port']}/{settings['db_name']}"
        with pdm(db_query, settings['table_name']) as dm:
            dm.migrate()


    def setUp(self):
        self.settings = json_read('test_config.json')
        self.db_query = f"clickhouse://{self.settings['db_user_name']}:{self.settings['db_password']}@{self.settings['db_ip']}:{self.settings['db_port']}/{self.settings['db_name']}"
        with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"TRUNCATE TABLE {self.settings['table_name']}")
        

    def test_insert(self):
        obj = {"time": "2020-10-27 14:45:00",
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
        with pdm(self.db_query, self.settings['table_name']) as dm:
            dm.insert_val(obj, 'tests/resources/access_mini.log')
        with connect(self.db_query) as cnx:
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
                        proxy_host FROM """+f'{self.settings["table_name"]}')
        row = cur.fetchone()
        self.assertTrue(row == tuple(obj.values()))

    def test_double_insert(self):
        obj = {"time": "2021-10-27 14:45:42",
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
        with pdm(self.db_query, self.settings['table_name']) as dm:
            for i in range(2):
                dm.insert_val(obj, 'tests/resources/access_mini.log')
        with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"SELECT * FROM {self.settings['table_name']}")
            row = cur.fetchall()
            self.assertTrue(len(row) == 1)

    def test_double_insert_false(self):
        obj = dict({"time": """\2021-10-27 14:45:42""",
               "remote_addr": "103.42.20.221",
               "remote_user": "03039",
               "body_bytes_sent": "162",
               "request_time": "0.000",
               "status": "301",
               "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1",
               "request_method": "POST",
               "http_referrer": "-",
               "http_user_agent": "SQLAnywhere/16.0.0.2546",
               "proxy_host": "-"})
        with pdm(self.db_query, self.settings['table_name']) as dm:
            for i in range(2):
                dm.false_insert_val()
            with connect(self.db_query) as cnx:
                cur = cnx.cursor()
                cur.execute(f"SELECT * FROM {self.settings['table_name']}")
                row = cur.fetchall()
                self.assertTrue(len(list(row)) == 2)


if __name__ == "__main__":
    unittest.main()
