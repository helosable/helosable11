import re
import numpy
from log_analyzer.services.country_getter import country_name_from_ip


class Per_report():

    def __init__(self, dm):
        self.dm = dm

    def render(self, first_time, second_time):
        percentile_list = [50, 75, 95, 99]
        report_list = [['func_name', '50 per', '90 per', '95 per', '99 per', 'location']]

        func_name_list = self.dm.fetch_request_time_status_by_time(first_time, second_time)
        for current_row in func_name_list:
            if current_row[1] == '404':
                continue

            if current_row == 'error':
                continue
            location = country_name_from_ip(func_name_list[0][-1])


            time = self.dm.fetch_request_time_by_fname(current_row[0])

            current_time_list = []
            current_percentile_list = []

            current_percentile_list.append(re.split('[(?|;)]', current_row[0])[0])

            for current_time in time:
                current_time_list.append(float(current_time[0]))

            for percentile in percentile_list:
                current_percentile = float(numpy.percentile(current_time_list, percentile))

                if len(f'{current_percentile}') > 6:
                    current_percentile = float('{:.5f}'.format(current_percentile))

                current_percentile_list.append(current_percentile)
            current_percentile_list.append(f"{location[0]}, {location[1]}")
            report_list.append(current_percentile_list)

        return report_list
