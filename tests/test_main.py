import unittest
from clickhouse_driver import connect
from log_analyzer.models.parser_data_manager import Parser_data_manager
from main import *


class main_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        settings = json_read('test_config.json')
        db_query = f"clickhouse://{settings['db_user_name']}:{settings['db_password']}@{settings['db_ip']}:{settings['db_port']}/{settings['db_name']}"
        with Parser_data_manager(db_query, settings['table_name']) as dm:
            dm.migrate()


    def setUp(self):
        self.settings = json_read('test_config.json')
        self.db_query = f"clickhouse://{self.settings['db_user_name']}:{self.settings['db_password']}@{self.settings['db_ip']}:{self.settings['db_port']}/{self.settings['db_name']}"
        parse_log_file(self.db_query, self.settings['table_name'], 'tests/resources/access_mini.log')
        # print('lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll')
        self.table_name = self.settings['table_name']

    def tearDown(self):
       with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"TRUNCATE TABLE {self.table_name}")
            # print('yyyyyyyyyyyyyyyyyyy')

    def test_main(self):
        # parse_log_file(self.db_query, self.settings['table_name'], 'tests/resources/access_mini.log')
        with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"SELECT * FROM {self.table_name}")
            notes = cur.fetchall()
            # print(notes)
            self.assertTrue(len(notes[0]) == 14)

    def test_main_false_insert(self):
        with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"SELECT * FROM {self.table_name}")
            notes = cur.fetchall()
            self.assertTrue(str(notes[0][1:2]) == "('error',)")

    def test_main_double_false_insert(self):
        with connect(self.db_query) as cnx:
            cur = cnx.cursor()
            cur.execute(f"SELECT * FROM {self.table_name} WHERE time='error'")
            notes = cur.fetchall()
            # print(notes)
            self.assertTrue(len(notes) > 2)

    def test_json_read(self):
        settings = json_read('test_config.json')
        self.assertTrue(settings['db_name'] == 'my_db')

    def test_good_args(self):
        args = parse_args(['-rep', 'ip_report', '-first_time', '2020-10-27 14:45:42',
                                '-second_time', '2020-10-28 23:55:46', '-f', 'false.log'])
        self.assertTrue(args.rep == 'ip_report')

    # def test_bad_time(self):
    #     try:
    #         self.assertTrue(time_check('2020-10-27 14:45:42', '2020-14-27 14:45:42') is False)
    #     except Exception:
    #         test_res = False
    #     self.assertTrue(test_res is False)

    def test_bad_config(self):
        bad_config = {"db_name": ""}
        self.assertTrue(settings_check(bad_config) is False)
