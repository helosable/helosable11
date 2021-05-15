import jinja2
from reports.factories.factory_ip_report import Factory_ip_report
from reports.factories.factory_per_report import Factory_per_report


class Renderer():
    def __init__(self, report_name):
        self.report_name = report_name

    def report_choise(self):
        report_list = ['ip_report', 'per_report']
        if self.report_name not in report_list:
            return 1
        factories = {'ip_report': Factory_ip_report(),
                     'per_report': Factory_per_report()}
        return factories[self.report_name]

    def main_render(self, first_time, second_time, db_name):
        report = self.report_choise().produce(db_name).render(str(first_time), str(second_time))
        file_name = str(self.report_name)
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{file_name}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))
