import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import time
import sys
import sqlite3
import numpy

first_time = sys.argv[1:]
second_time = sys.argv[2:]


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def migrate():
    backend = get_backend("sqlite:///main.db")
    migrations = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))

end = time.process_time()


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

def report(first_time='2020-10-27T14:45:42+00:00', second_time='2020-10-27T14:45:43+00:00'):
    with sqlite3.connect('tests/resources/test.db') as cnx:
        cur = cnx.cursor()
        time_first = f"{first_time}"
        time_second = f"{second_time}"
        rep_list = []
        per_list = [50, 75, 95, 99]
        func_name = list(cur.execute(f'SELECT request FROM my_table WHERE time BETWEEN {time_first} AND {time_second} GROUP BY request'))
        for func in func_name:
            mass = cur.execute(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
            add_list = []
            add_list.append(str(func))
            for i in per_list:
                add_list.append(int(numpy.percentile(list(mass), i)))
            rep_list.append(add_list)
    return rep_list

if __name__ == "__main__":
    migrate()
    main("main.db", 'tests/resources/access_mini_false.log')
    print(report())
    
