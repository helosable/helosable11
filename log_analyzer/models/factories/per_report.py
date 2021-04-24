import sys
import os
import numpy
sys.path.append(os.getcwd())
from log_analyzer.models.parser_data_manager import Parser_data_manager


class Per_report(Parser_data_manager):
    def render(self, first_time, second_time):
        percentile_list = [50, 75, 95, 99]
        report_list = [['func_name', '50 per', '90 per', '95 per', '99 per']]
        func_name_list = self.val_return_for_per_report(first_time, second_time)
        for current_row in func_name_list:
            if current_row[1] == '404':
                continue
            if current_row == 'error':
                continue
            time = self.val_return_for_per_report(None, None, True, current_row[0])
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
