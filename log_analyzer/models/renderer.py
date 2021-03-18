from .parser_data_manager import Parser_data_manager
import numpy
import jinja2
import re


class Renderer:
    def __init__(self, connection_string):
        self.dm = Parser_data_manager(connection_string)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dm.close()

    def func_name_change(self, func):
        return re.split('[(?|;)]', func)[0]

    def main_render(self, report_name, first_time, second_time):
        if report_name == 'ip_report':
            report = self.ip_report(first_time, second_time)
            file_name = str(report_name)
        if report_name == 'per_report':
            report = self.per_report(first_time, second_time)
            file_name = str(report_name)
        headings = report[0]
        data = report[1:]
        with open(f'./jinja/templates/{file_name}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))

    def per_report(self, first_time, second_time):
        percentile_list = [50, 75, 95, 99]
        report_list = [['func_name', '50 per', '90 per', '95 per', '99 per']]
        func_name_list = self.dm.val_return_for_per_report(first_time, second_time)
        for current_row in func_name_list:
            if current_row[1] == '404':
                continue
            if current_row == 'error':
                continue
            time = self.dm.val_return_for_per_report(None, None, True, current_row[0])
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

    def ip_report(self, first_time, second_time):
        report_list = [['time', 'func', 'ip']]
        values = self.dm.val_return_for_ip_report(first_time, second_time)
        for current_row in values:
            if current_row[3] == '404':
                continue
            if current_row[0] == 'error':
                continue
            report_list.append([current_row[0], self.func_name_change(current_row[1]), current_row[2]])
        return report_list
