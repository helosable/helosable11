import numpy
import jinja2
import re
import ijson
import sys
import os
sys.path.append(f'{os.getcwd()}/log_analyzer/models/factories')
from factory_ip_report import Factory_ip_report
from factory_per_report import Factory_per_report

class Renderer():

    def main_render(self, report_name, first_time, second_time, db_name):
        factories = {'ip_report': Factory_ip_report(),
                    'per_report': Factory_per_report()}     
        first_time = f'{first_time}'
        second_time = f'{second_time}'
        report = factories[report_name].produce(db_name).render(first_time, second_time)
        # print(report)
        file_name = str(report_name)
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{file_name}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))

    def per_report(self, first_time, second_time):
        percentile_list = [50, 75, 95, 99]
        report_list = [['func_name', '50 per', '90 per', '95 per', '99 per']]
        func_name_list = self.val_return_for_per_report(first_time, second_time)
        print(func_name_list)
        time = self.val_return_for_per_report(None, None, True, func_name_list)
        for current_row in func_name_list:
            if current_row[1] == '404':
                continue
            if current_row == 'error':
                continue
            current_time_list = []
            current_percentile_list = []
            current_percentile_list.append(self.func_name_change(current_row[0]))
            for current_time in time:
                current_time_list.append(float(current_time[0]))
            for percentile in percentile_list:
                current_percentile = float(numpy.percentile(current_time_list, percentile))
                if len(f'{current_percentile}') > 6:
                    current_percentile = float('{:.5f}'.format(current_percentile))
                current_percentile_list.append(current_percentile)
            report_list.append(current_percentile_list)
        return report_list    

    @staticmethod
    def func_name_change(func):
        return re.split('[(?|;)]', func)[0]

    @staticmethod
    def json_still_valid(js):
        try:
            return next(ijson.items(js, "", multiple_values=True))
        except ijson.common.IncompleteJSONError:
            return False