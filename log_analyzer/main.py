import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import jinja2
import argparse


def args():
    parser = argparse.ArgumentParser()
    try:
        parser.add_argument('-first_time', '-f_t', '--f_time', type=str, default='2020-10-27 14:45:42')
        parser.add_argument('-second_time', '-s_t', '--s_time', type=str, default='2020-10-27 14:45:43')
        parser.add_argument('-file', '-f', '--log_file', type=str, default='access.log')
        parser.add_argument('-rep', '-r', '--rep', type=str, required=True)
        args = parser.parse_args()
    except argparse.ArgumentError:
        print('bad args')
        raise NameError
    return args

with open('config.json', 'r') as config:
    settings_data = next(ijson.items(config, '', multiple_values=True))
    db_name = settings_data['db']
    api_token = settings_data['api_token']


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def migrate(db_name):
    backend = get_backend(f"sqlite:///{db_name}")
    migration = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migration))


def main(db_name, file_name):
    try:
        with open(file_name, "r") as myfile:
            with Parser_data_manager(db_name) as dm:
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    dm.insert_val(row, file_name)

    except FileNotFoundError:
        print(f"file '{file_name}' not found")
    except Exception as e:
        print(repr(e))


def render(report_name, db_name=db_name):
    with Parser_data_manager(db_name) as dm:
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
    args = args()
    migrate(db_name)
    main(db_name, args.log_file)
    render(args.rep)
