import unittest
import sqlite3
from yoyo import read_migrations, get_backend
from log_analyzer.models.renderer import Renderer


class main(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        backend = get_backend("sqlite:///tests/resources/test_renderer.db")
        migrations = read_migrations("./migrations")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def setUp(self):
        with sqlite3.connect('tests/resources/test_renderer.db') as cnx:
            cnx.execute("""DELETE FROM my_table""")
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../log_analyzer")
        from log_analyzer import main
        main.parse_log_file('tests/resources/test_renderer.db', 'tests/resources/access_mini_false.log')

    def test_per_report(self):
        with Renderer('tests/resources/test_renderer.db') as ren:
            rep = ren.per_report('2020-10-27 14:45:42', '2020-10-27 14:45:43')
        true_list = [0.216, 0.216, 0.216, 0.216]
        self.assertTrue(rep[1][1:] == true_list)
