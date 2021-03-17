from .parser_data_manager import Parser_data_manager
import numpy
import jinja2


class Renderer:
    def __init__(self, connection_string):
        self.dm = Parser_data_manager(connection_string)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dm.close()

    def func_name(self, func):
        new_func = ''
        for i in func:
            for l1 in i:
                if l1 == '?' or l1 == ';':
                    l1 = ''
                    return(new_func)
                new_func += l1
        return new_func

    def main_render(self, report_name, first_time, second_time):
        if report_name == 'ip_report':
            rep = self.ip_report(first_time, second_time)
            file_name = str(report_name)
        if report_name == 'per_report':
            rep = self.per_report(first_time, second_time)
            file_name = str(report_name)
        headings = rep[0]
        data = rep[1:]
        with open(f'./jinja/templates/{file_name}.html', 'w') as myfile:
            myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('./jinja/templates'))
                         .get_template('base.html').render(heading=headings, data=data))

    def per_report(self, first_time, second_time):
        per_list = [50, 75, 95, 99]
        time_list = [['func_name', '50 per', '90 per', '95 per', '99 per']]
        func_name = self.dm.val_return_with_time("""SELECT request, status FROM my_table request
        WHERE time BETWEEN ? AND ? GROUP BY request""", first_time, second_time)
        for func in func_name:
            func = func[0]
            if func[1] == '404':
                print(404)
                continue
            if func == 'error':
                continue
            time = self.dm.val_return(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
            new_time = []
            new_per_list = []
            new_per_list.append(self.func_name(func))
            for i in time:
                new_time.append(float(i[0]))
            for i in per_list:
                i1 = float(numpy.percentile(new_time, i))
                if len(f'{i1}') > 6:
                    i1 = float('{:.5f}'.format(i1))
                new_per_list.append(i1)
            time_list.append(new_per_list)
        return time_list

    def ip_report(self, first_time, second_time):
        rep_list = []
        rep_list.append(['time', 'func', 'ip'])
        ip_name = self.dm.val_return_with_time("""SELECT time, request, remote_addr, status FROM my_table
        WHERE time BETWEEN ? AND ? GROUP BY request""", first_time, second_time)
        for ip in ip_name:
            if ip[3] == '404':
                continue
            if ip == 'error':
                continue
            rep_list.append([ip[0], self.func_name(ip[1]), ip[2]])
        return rep_list
