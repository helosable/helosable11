import jinja2
import sys
import os
from reports.factories.factory_ip_report import Factory_ip_report
from reports.factories.factory_per_report import Factory_per_report


class Renderer():

    def main_render(self, report_name, first_time, second_time, db_name):
        report_list = ['ip_report', 'per_report']
        if report_name not in report_list: return 1
        factories = {'ip_report': Factory_ip_report(),
                    'per_report': Factory_per_report()}
        first_time = f'{first_time}'
        second_time = f'{second_time}'
        report = factories[report_name].produce(db_name).render(first_time, second_time)
        file_name = str(report_name)
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{file_name}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                        .get_template('base.html').render(heading=headings, data=data))