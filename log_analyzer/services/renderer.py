import jinja2
from datetime import date
from log_analyzer.reports.factories.factory_ip_report import Factory_ip_report
from log_analyzer.reports.factories.factory_per_report import Factory_per_report


class Renderer():
    def __init__(self, dm, report_name):
        self._dm = dm
        self._report_name = report_name

    def _report_name_check(self):
        report_list = ['ip_report', 'per_report']
        return self._report_name in report_list

    def _report_choise(self):
        if self._report_name_check() is False:
            raise Exception("Unknown report")

        factories = {'ip_report': Factory_ip_report(),
                     'per_report': Factory_per_report()}
        return factories[self._report_name]

    def process(self, first_time, second_time, settings):
        report = self._report_choise().produce(self._dm).render(
            first_time, second_time)

        headings = report[0]
        data = report[1:]
        with open(f'./jinja/results/{self._report_name}-{date.today().strftime("%Y-%m-%d")}.html', 'w') as myfile:  # noqa:E501
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))
