import unittest
import sqlite3
from yoyo import read_migrations, get_backend


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
        from log_analyzer.reports.factories.factory_per_report import Factory_per_report
        self.factories = {'per_report': Factory_per_report()}

    def test_per_report(self):
        db = 'tests/resources/test_renderer.db'
        rep = self.factories['per_report'].produce(db).render('2020-10-27 14:45:42', '2020-10-27 14:45:43')
        true_list = [0.216, 0.216, 0.216, 0.216]
        self.assertTrue(rep[1][1:] == true_list)

    def test_bad_args_for_report(self):
        from log_analyzer.services.renderer import Renderer
        ren = Renderer('sqlite:///tests/resources/test_renderer.db')
        try:
            ren._report_choise('bad_report')
        except Exception:
            error = 1
        self.assertTrue(error == 1)
