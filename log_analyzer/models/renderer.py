from .parser_data_manager import Parser_data_manager
import numpy
import jinja2
import re
import ijson


class Renderer():

    def func_name_change(self, func):
        return re.split('[(?|;)]', func)[0]


    def main_render(self, report_name, first_time, second_time):
        self.import_factory()
        # print(f'self.{report_name}({first_time}, {second_time})')
        # report = exec(f'self.{report_name}({first_time}, {second_time})')
        # report = self.ip_report('2020-10-27 14:45:42', '2020-10-27 14:45:43')
        first_time = f'{first_time}'
        second_time = f'{second_time}'
        # report = exec("f'self.ip_report({first_time}, {second_time})'")
        report = Factory_ip_report().reduce.render(first_time, second_time)
        print(report)
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

    def import_factory(self):
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "\\factories")
        # print (sys.path)
        from factory_ip_report import Factory_ip_report      

    @staticmethod
    def json_still_valid(js):
        try:
            return next(ijson.items(js, "", multiple_values=True))
        except ijson.common.IncompleteJSONError:
            return False