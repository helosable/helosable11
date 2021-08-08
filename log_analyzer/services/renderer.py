import jinja2
from reports.factories.factory_ip_report import Factory_ip_report
from reports.factories.factory_per_report import Factory_per_report


class Renderer():
    def __init__(self, db_name, report_name):
        self.db_name = db_name
        self.report_name = report_name

    def report_name_check(self):
        report_list = ['ip_report', 'per_report']
        if self.report_name not in report_list:
            return False

    def _report_choise(self):
        if self.report_name_check() is False:
            raise Exception("Unknown report")
        factories = {'ip_report': Factory_ip_report(),
                     'per_report': Factory_per_report()}
        return factories[self.report_name]

    def process(self, first_time, second_time, api_token):
        report = self._report_choise().produce(self.db_name).render(
            str(first_time), str(second_time), api_token)
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{str(self.report_name)}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))
