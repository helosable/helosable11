import jinja2
import re
import ijson
import sys
import os


class Renderer():

    def main_render(self, report_name, first_time, second_time, db_name):
        sys.path.append(f'{os.getcwd()}/log_analyzer/models/factories')
        from factory_ip_report import Factory_ip_report
        from factory_per_report import Factory_per_report
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

    @staticmethod
    def func_name_change(func):
        return re.split('[(?|;)]', func)[0]

    @staticmethod
    def json_still_valid(js):
        try:
            return next(ijson.items(js, "", multiple_values=True))
        except ijson.common.IncompleteJSONError:
            return False
