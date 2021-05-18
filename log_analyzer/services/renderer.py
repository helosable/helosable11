import jinja2
from reports.factories.factory_ip_report import Factory_ip_report
from reports.factories.factory_per_report import Factory_per_report


class Renderer():
    def __init__(self, db_name):
        self.db_name = db_name

    def _report_choise(self, report_name):
        report_list = ['ip_report', 'per_report']
        if report_name not in report_list:
            raise Exception("Unknown report")

        factories = {'ip_report': Factory_ip_report(),
                     'per_report': Factory_per_report()}
        return factories[report_name]

    def process(self, first_time, second_time, report_name):
        report = self._report_choise(report_name).produce(self.db_name).render(str(first_time), str(second_time))
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{str(report_name)}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))
