def import_pdm():
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.realpath(__file__)) + os.path.dirname(os.path.realpath(__file__))[-10:])
    # from parser_data_manager import Parser_data_manager
    print(sys.path)
    print(len(sys.path))

# class Ip_report(Parser_data_manager):
#     def render(self, first_time, second_time):
#         report_list = [['time', 'func', 'ip']]
#         values = self.val_return_for_ip_report(first_time, second_time)
#         for current_row in values:
#             if current_row[3] == '404':
#                 continue
#             if current_row[0] == 'error':
#                 continue
#             report_list.append([current_row[0], self.func_name_change(current_row[1]), current_row[2]])
#         return report_list

import_pdm()