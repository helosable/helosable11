import jinja2
import sys
import os


class Renderer():

    def main_render(self, report_name, first_time, second_time, db_name):
        sys.path.append(os.getcwd())
        from log_analyzer.reports.factories.factory_ip_report import Factory_ip_report
        from log_analyzer.reports.factories.factory_per_report import Factory_per_report
        report_list = ['ip_report', 'per_report']
        if report_name in report_list:
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
            return None
        else:
            print('bad_agrs')
            return 1
