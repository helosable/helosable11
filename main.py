import os
import sys
import ijson
import argparse
from yoyo import read_migrations, get_backend
from datetime import datetime
from log_analyzer.models.parser_data_manager import Parser_data_manager
from log_analyzer.services.renderer import Renderer


def datetime_validate(datetime_str):
    datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-first_time', '-f_t', '--f_time', type=str)
    parser.add_argument('-second_time', '-s_t', '--s_time', type=str)
    parser.add_argument('-file', '-f', '--log_file', type=str, default='access.log')
    parser.add_argument('-rep', '-r', '--rep', type=str)

    parsed_args = parser.parse_args(args)
    if parsed_args.rep:
        parsed_args.f_time and datetime_validate(parsed_args.f_time)
        parsed_args.s_time and datetime_validate(parsed_args.s_time)

    return parsed_args


def migrate(db_name):
    backend = get_backend(db_name)
    migration = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migration))


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def settings_check(js):
    if len(js["db"]) == 0:
        print("DB connection string is empty")
        return False
    return True


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
                else:
                    dm.insert_val(row, file_name)

if __name__ == "__main__":
    settings = json_read()

    if settings_check(settings) is False:
        print("Bad config")
        sys.exit(1)

    migrate(settings['db'])
    try:
        args = parse_args(sys.argv[1:])

        if args.log_file:
            if not os.path.exists(args.log_file):
                raise Exception('file for parsing not found')

            parse_log_file(settings['db'], args.log_file)

        if args.rep:
            with Parser_data_manager(settings['db']) as dm:
                render = Renderer(dm, args.rep)
                render.process(args.f_time, args.s_time, settings)

    except Exception as error:
        print(error)
        sys.exit(1)

    sys.exit(0)
