import ijson
from models.parser_data_manager import Parser_data_manager
from models.renderer import Renderer
import argparse
import sys
import os
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


def parse_log_file(db_name, file_name):
    with Parser_data_manager(db_name) as dm:
        dm.migrate(db_name)
        with open(file_name, "r") as myfile:
            for line in myfile:
                row = Renderer.json_still_valid(line)
                if not row:
                    dm.false_insert_val(line)
                    continue
                dm.insert_val(row, file_name)


if __name__ == "__main__":
    settings = json_read()
    try:
        args = parse_args(sys.argv[1:])
        if os.path.exists(args.log_file):
            render = Renderer()
            if args.rep_only:
                render.main_render(args.rep, args.f_time, args.s_time, settings['db'])
                print(0)
                sys.exit()
            parse_log_file(settings['db'], args.log_file)
            render.main_render(args.rep, args.f_time, args.s_time, settings['db'])
        else:
            print('1, file for parsing was not found')
            sys.exit
    except (argparse.ArgumentTypeError, sqlite3.OperationalError, TypeError, FileNotFoundError) as error:
        print(1, error)
        sys.exit()
    print(0)
    sys.exit()
