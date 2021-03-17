import ijson
from models.parser_data_manager import Parser_data_manager
from models.renderer import Renderer
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
    parser.add_argument('-parse_only', '--parse_only', action='store_true')
    return parser.parse_args(args)


def time_parse(time):
    new_time = ''
    for i in time:
        if i == '_':
            i = ' '
        new_time += i
    return new_time


def json_read():
    with open('config.json', 'r') as config:
        return next(ijson.items(config, '', multiple_values=True))


def parse_log_file(db_name, file_name):
    with Parser_data_manager(db_name) as dm:
        dm.migrate()
        with open(file_name, "r") as myfile:
            for line in myfile:
                row = dm.json_still_valid(line)
                if not row:
                    dm.false_insert_val(line)
                    continue
                dm.insert_val(row, file_name)


if __name__ == "__main__":
    settings = json_read()
    with Renderer(settings['db']) as render:
        try:
            args = parse_args(sys.argv[1:])
            if args.rep_only:
                render.main_render(args.rep, time_parse(args.f_time), time_parse(args.s_time))
                sys.exit()
            if args.parse_only:
                parse_log_file(settings['db'], args.log_file)
                sys.exit()
            parse_log_file(settings['db'], args.log_file)
            render.main_render(args.rep, time_parse(args.f_time), time_parse(args.s_time))
        except (argparse.ArgumentTypeError, sqlite3.OperationalError, TypeError, FileNotFoundError) as error:
            print(error)
            sys.exit()
        sys.exit()
