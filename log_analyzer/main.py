import ijson
from models.parser_data_manager import Parser_data_manager
import jinja2
import argparse
import sys
import sqlite3


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-first_time', '-f_t', '--f_time', type=str)
    parser.add_argument('-second_time', '-s_t', '--s_time', type=str)
    parser.add_argument('-file', '-f', '--log_file', type=str, default='access.log')
    parser.add_argument('-rep', '-r', '--rep', type=str, required=True)
    parser.add_argument('-report_only', '--rep_only', action='store_true')
    return parser.parse_args(args)


def json_read():
    with open('config.json', 'r') as config:
        return next(ijson.items(config, '', multiple_values=True))


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def parse_log_file(db_name, file_name, report_name='ip_report'):
    with Parser_data_manager(db_name) as dm:
        dm.migrate()
        with open(file_name, "r") as myfile:
            for line in myfile:
                row = json_still_valid(line)
                if not row:
                    dm.false_insert_val(line)
                    continue
                dm.insert_val(row, file_name)


def render(report_name):
    with Parser_data_manager(settings['db']) as dm:
        args = parse_args(sys.argv[1:])
        if report_name == 'ip_report':
            rep = dm.ip_report(args.f_time, args.s_time)
            file_name = str(report_name)
        if report_name == 'per_report':
            rep = dm.per_report(args.f_time, args.s_time)
            file_name = str(report_name)
    headings = rep[0]
    data = rep[1:]
    with open(f'jinja/templates/{file_name}.html', 'w') as myfile:
        myfile.write(jinja2.Environment(loader=jinja2.FileSystemLoader('jinja/templates'))
                     .get_template('base.html').render(heading=headings, data=data))


if __name__ == "__main__":
    settings = json_read()
    try:
        args = parse_args(sys.argv[1:])
        if not args.rep_only:
            parse_log_file(settings['db'], args.log_file)
        
        render(args.rep)
    except (argparse.ArgumentTypeError, sqlite3.OperationalError, TypeError, FileNotFoundError) as error:
        print(error)
        sys.exit(1)
    sys.exit(0)
