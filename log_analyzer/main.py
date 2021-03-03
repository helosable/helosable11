import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import jinja2
import argparse


parser = argparse.ArgumentParser()
try:
    parser.add_argument('-first_time', '-f_t', '--f_time', type = str)
    parser.add_argument('-second_time', '-s_t', '--s_time', type = str)
    parser.add_argument('-file', '-f', '--log_file', type = str)
    parser.add_argument('-rep', '-r', '--rep', type = str)
    args = parser.parse_args()
except :    
    print('args error')


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
                count = 0
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    dm.insert_val(row, file_name)
                    count += 1

    except FileNotFoundError:
        print("file 'access.log' not found")
    except Exception as e:
        print(repr(e))

def render(report_name, db_name = db_name):
    with Parser_data_manager(db_name, args.f_time, args.s_time) as dm:
        if report_name == 'ip_report':
            rep = dm.ip_report()
            file_name = str(report_name)
        if report_name == 'per_report':
            rep = dm.per_report()
            file_name = str(report_name)
    headings = rep[0]
    data = rep[1:]
    with open(f'jinja/templates/{file_name}.html', 'w') as myfile:
        myfile.write(jinja2.Environment(loader = jinja2.FileSystemLoader('jinja/templates')).get_template('base.html').render(heading = headings, data = data))


if __name__ == "__main__":
    migrate(db_name)
    with Parser_data_manager(db_name) as dm:
        dm.second_migration()
    main(db_name, args.log_file)
    render(args.rep)
