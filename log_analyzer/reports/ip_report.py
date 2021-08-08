from models.parser_data_manager import Parser_data_manager
import re
from reports.country_getter import country_name_from_ip


class Ip_report():

    def __init__(self, db):
        self.dm = Parser_data_manager(db)

    def render(self, first_time, second_time, api_key):
        report_list = [['time', 'func', 'ip', 'location']]
        values = self.dm.fetch_requests(first_time, second_time)
        for current_row in values:
            if current_row[3] == '404':
                continue
            if current_row[0] == 'error':
                continue
            location = country_name_from_ip(current_row[2], api_key)
            report_list.append([current_row[0], re.split('[(?|;)]', current_row[1])[0],
                                current_row[2], f"{location[0]}, {location[1]}"])
        return report_list
