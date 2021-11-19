import unittest
import sqlite3
from yoyo import read_migrations, get_backend
import main


class main_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///tests/resources/test_main.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def tearDown(self):
        with sqlite3.connect('tests/resources/test_main.db') as cnx:
            cnx.execute("""DELETE FROM my_table""")

    def setUp(self):
        main.parse_log_file('tests/resources/test_main.db', 'tests/resources/access_mini_false.log')

    def test_main(self):
        main.parse_log_file('tests/resources/test_main.db', 'tests/resources/access_mini_false.log')

        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(len(list(notes.fetchall())) == 11)

    def test_main_false_insert(self):
        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(list(notes)[1][2] == "error")

    def test_main_double_false_insert(self):
        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table WHERE time='error'")
            self.assertTrue(len(list(notes.fetchall())) == 2)

    def test_json_read(self):
        settings = main.json_read()
        self.assertTrue(settings['db'] == 'sqlite:///main.db')

    def test_good_args(self):
        args = main.parse_args(['-rep', 'ip_report', '-first_time', '2020-10-27 14:45:42',
                                '-second_time', '2020-10-28 23:55:46', '-f', 'false.log'])
        self.assertTrue(args.rep == 'ip_report')

    def test_bad_time(self):
        try:
            self.assertTrue(main.time_check('2020-10-27 14:45:42', '2020-14-27 14:45:42') is False)
        except Exception:
            test_res = False
        self.assertTrue(test_res is False)

    def test_bad_config(self):
        bad_config = {"db": ""}
        self.assertTrue(main.settings_check(bad_config) is False)
