import unittest
import sqlite3
from yoyo import read_migrations, get_backend


class main_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///tests/resources/test_main_false_insert.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def test_main(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        main.main("tests/resources/access_mini.log", "tests/resources/test_main_false_insert.db")

        with sqlite3.connect("tests/resources/test_main_false_insert.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(len(list(notes)) == 10)
    
    def test_main_false_insert(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
        from log_analyzer import main
        main.main("tests/resources/access_mini_false.log", "tests/resources/test_main_false_insert.db")
        with sqlite3.connect("tests/resources/test_main_false_insert.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(len(list(notes)) == 10)
