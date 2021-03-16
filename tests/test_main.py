import unittest
import sqlite3
from yoyo import read_migrations, get_backend
from log_analyzer.models.parser_data_manager import Parser_data_manager as pdm


class main_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///tests/resources/test_main.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def setUp(self):
        with sqlite3.connect('tests/resources/test_main.db') as cnx:
            cnx.execute("""DELETE FROM my_table""")
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        main.main('tests/resources/test_main.db', 'tests/resources/access_mini_false.log')

    def test_main(self):
        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(len(list(notes)) == 11)

    def test_main_false_insert(self):
        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(list(notes)[1][2] == "error")

    def test_main_double_false_insert(self):
        with sqlite3.connect("tests/resources/test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table WHERE time='error'")
            self.assertTrue(len(list(notes)) == 2)

    def test_report(self):
        with pdm('tests/resources/test_main.db') as dm:
            rep = dm.per_report('2020-10-27 14:45:42', '2020-10-27 14:45:43')
        true_list = [0.216, 0.216, 0.216, 0.216]
        self.assertTrue(rep[1][1:] == true_list)

    def test_json_read(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        settings = main.json_read()
        self.assertTrue(settings['db'] == 'main.db')

    def test_good_args(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        args = main.parse_args(['--rep', 'ip_report'])
        self.assertTrue(args.rep == 'ip_report')

    def test_bad_args(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        args = main.parse_args([])
        self.assertTrue(args is False)
