import ijson
from models.parser_data_manager import Parser_data_manager
from models.renderer import Renderer
from yoyo import read_migrations, get_backend
import argparse
import sys
import os
import re


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-first_time', '-f_t', '--f_time', type=str)
    parser.add_argument('-second_time', '-s_t', '--s_time', type=str)
    parser.add_argument('-file', '-f', '--log_file', type=str, default='access.log')
    parser.add_argument('-rep', '-r', '--rep', type=str, required=True)
    parser.add_argument('-report_only', '--rep_only', action='store_true')
    return parser.parse_args(args)


def migrate(db_name):
    backend = get_backend(f"sqlite:///{db_name}")
    migration = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migration))


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def json_read():
    with open('config.json', 'r') as config:
        return next(ijson.items(config, '', multiple_values=True))


def parse_log_file(db_name, file_name):
    with Parser_data_manager(db_name) as dm:
        with open(file_name, "r") as myfile:
            for line in myfile:
                row = json_still_valid(line)
                if not row:
                    dm.false_insert_val(line)
                    continue
                dm.insert_val(row, file_name)


if __name__ == "__main__":
    settings = json_read()
    migrate(settings['db'])
    try:
        args = parse_args(sys.argv[1:])
        if os.path.exists(args.log_file):
            render = Renderer(args.rep)
            good_date = r'20[0-9][0-9]-(1[0-2]|0[1-9])-([0-2][0-9]|3[0-1])\s[0-2][0-59]:[0-5][0-9]:[0-5][0-9]'
            if not bool(re.
                        match(good_date, args.f_time)) or not bool(re.match(good_date, args.s_time)):
                print('bad date or time')
                sys.exit(1)
            if args.rep_only:
                render_result = render.main_render(args.f_time, args.s_time, settings['db'])
                if render_result == 1:
                    print('1, bad args')
                    sys.exit(1)
                print(0)
                sys.exit()
            parse_log_file(settings['db'], args.log_file)
            render_result = render.main_render(args.f_time, args.s_time, settings['db'])
            if render_result == 1:
                print('1, bad args')
                sys.exit(1)
        else:
            print('1, file for parsing was not found')
            sys.exit(1)
    except (argparse.ArgumentTypeError, TypeError, FileNotFoundError) as error:
        print(1, error)
        sys.exit(1)
    print(0)
    sys.exit(0)
