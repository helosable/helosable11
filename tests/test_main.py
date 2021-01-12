import unittest
import log_analyzer.models.main as main
import sqlite3
from yoyo import read_migrations, get_backend


class main_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///test_main.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def test_main(self):
        main.main("acces_mini.log", "test_main.db")
        with sqlite3.connect("test_main.db") as cnx:
            cur = cnx.cursor()
            notes = cur.execute("SELECT * FROM my_table")
            self.assertTrue(len(list(notes)) == 10)
