import unittest
from yoyo import read_migrations, get_backend
import sqlite3
import json

from log_analyzer.models.parser_data_manager import Parser_data_manager
from log_analyzer.reports.factories.factory_per_report import Factory_per_report


class main(unittest.TestCase):
    _db_name = "sqlite:///tests/resources/test_renderer.db"

    @classmethod
    def setUpClass(cls):
        backend = get_backend(main._db_name)
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def setUp(self):
        with sqlite3.connect("tests/resources/test_renderer.db",
                             detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:
            cur = con.cursor()
            con.execute("""DELETE FROM my_table""")
            with open("tests/resources/access_mini.json") as fi:
                cur.executemany("""INSERT OR IGNORE INTO my_table (
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
                row_hash,
                file_name
                ) VALUES (
                :time,
                :remote_addr,
                :remote_user,
                :body_bytes_sent,
                :request_time,
                :status,
                :request,
                :request_method,
                :http_referrer,
                :http_user_agent,
                :proxy_host,
                :row_hash,
                :file_name)""", json.load(fi)['data'])

    def tearDown(self):
        with sqlite3.connect('tests/resources/test_renderer.db') as cnx:
            cnx.execute("""DELETE FROM my_table""")

    def test_per_report(self):
        with Parser_data_manager('tests/resources/test_renderer.db') as dm:
            rep = Factory_per_report().produce(dm).render('2020-10-27 14:45:42', '2020-10-27 14:45:43')
            true_list = [0.03, 0.03, 0.03, 0.03]
            self.assertTrue(rep[1][1:5] == true_list)

    def test_bad_args_for_report(self):
        from log_analyzer.services.renderer import Renderer
        ren = Renderer('sqlite:///tests/resources/test_renderer.db', 'bad_report')
        try:
            ren._report_choise()
        except Exception:
            error = 1
        self.assertTrue(error == 1)
