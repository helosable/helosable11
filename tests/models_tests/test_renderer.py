import unittest
import json
from log_analyzer.models.parser_data_manager import Parser_data_manager
from log_analyzer.reports.factories.factory_per_report import Factory_per_report
from main import json_read, parse_log_file
from clickhouse_driver import connect
import hashlib
import collections


class main(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings = json_read('test_config.json')
        db_query = f"clickhouse://{settings['db_user_name']}:{settings['db_password']}@{settings['db_ip']}:{settings['db_port']}/{settings['db_name']}"
        with Parser_data_manager(db_query, settings['table_name']) as dm:
            dm.migrate()
        with connect(db_query) as con:
            cur = con.cursor()


    def setUp(self):
        self.settings = json_read('test_config.json')
        self.db_query = f"clickhouse://{self.settings['db_user_name']}:{self.settings['db_password']}@{self.settings['db_ip']}:{self.settings['db_port']}/{self.settings['db_name']}"
        parse_log_file(self.db_query, self.settings['table_name'], 'tests/resources/access_mini.log')
        with connect(self.db_query) as con:
            cur = con.cursor()
            cur.execute(f'select * from {self.settings["table_name"]}')

    def tearDown(self):
        with connect(self.db_query) as con:
            cur = con.cursor()
            print(cur.fetchall())
            

    def test_per_report(self):
        with Parser_data_manager(self.db_query, self.settings['table_name']) as dm:
            rep = Factory_per_report().produce(dm).render('2020-10-27 14:45:42', '2020-10-27 14:45:43')
            print('[[[[[[[[[[[[')
            print(rep)
            true_list = [0.0, 0.00, 0.00, 0.00]
            self.assertTrue(rep[1][1:5] == true_list)

    def test_bad_args_for_report(self):
        from log_analyzer.services.renderer import Renderer
        ren = Renderer(self.db_query, 'bad_report')
        try:
            ren._report_choise()
        except Exception:
            error = 1
        self.assertTrue(error == 1)
