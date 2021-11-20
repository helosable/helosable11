import os
import sys
import ijson
import argparse
from datetime import datetime
from log_analyzer.models.parser_data_manager import Parser_data_manager
from log_analyzer.services.renderer import Renderer


def datetime_validate(datetime_str):
    datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-first_time', '-f_t', '--f_time', type=str)
    parser.add_argument('-second_time', '-s_t', '--s_time', type=str)
    parser.add_argument('-file', '-f', '--log_file', type=str, default='acces_mini.log')
    parser.add_argument('-rep', '-r', '--rep', type=str)

    parsed_args = parser.parse_args(args)
    if parsed_args.rep:
        parsed_args.f_time and datetime_validate(parsed_args.f_time)
        parsed_args.s_time and datetime_validate(parsed_args.s_time)

    return parsed_args

def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def settings_check(js):
    if len(js["db_name"]) == 0:
        print("DB connection string is empty")
        return False
    return True


def json_read(config):
    with open(config, 'r') as config:
        return next(ijson.items(config, '', multiple_values=True))


def parse_log_file(db_adress, table_name, file_name):
    with Parser_data_manager(db_adress, table_name) as dm:
        dm.migrate()
        with open(file_name, "r") as myfile:
            for line in myfile:
                row = json_still_valid(line)
                if not row:
                    dm.false_insert_val()
                if dm.insert_val(row, file_name) == 0:
                    pass

if __name__ == "__main__":
    settings = json_read('config.json')
    db_query = f"clickhouse://{settings['db_user_name']}:{settings['db_password']}@{settings['db_ip']}:{settings['db_port']}/{settings['db_name']}"

    if settings_check(settings) is False:
        print("Bad config")
        sys.exit(1)
    try:
        args = parse_args(sys.argv[1:])

        if args.log_file:
            if not os.path.exists(args.log_file):
                raise Exception('file for parsing not found')

            parse_log_file(db_query, settings['table_name'], args.log_file)

        if args.rep:
            with Parser_data_manager(db_query, settings['table_name']) as dm:
                render = Renderer(dm, args.rep)
                render.process(args.f_time, args.s_time, settings)

    except Exception as error:
        print(error)
        sys.exit(1)

    sys.exit(0)
