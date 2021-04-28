from models.parser_data_manager import Parser_data_manager
import re


class Ip_report():

    def __init__(self, db):
        self.dm = Parser_data_manager(db)

    def render(self, first_time, second_time):
        report_list = [['time', 'func', 'ip']]
        values = self.dm.val_return_for_ip_report(first_time, second_time)
        for current_row in values:
            if current_row[3] == '404':
                continue
            if current_row[0] == 'error':
                continue
            report_list.append([current_row[0], re.split('[(?|;)]', current_row[1])[0], current_row[2]])
        print(report_list)
        return report_list
