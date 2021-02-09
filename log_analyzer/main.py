import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import time
import sys

first_time = sys.argv[1]
second_time = sys.argv[2]

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
            with Parser_data_manager(db_name, file_name) as dm:
                count = 0
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    dm.insert_val(row)
                    count += 1

    except FileNotFoundError:
        print("file 'access.log' not found")
    except Exception as e:
        print(repr(e))


def report():
    with Parser_data_manager("access.log", "main.db") as dm:
        func_list = dm.report_func(first_time, second_time)
        unique_list = []
        for word in func_list:
            if word is not unique_list:
                unique_list.append(word)
        for i in unique_list:
            print(f'функция {i} 50 перцентилей {dm.report(50, first_time, second_time)},')
            print(f'функция {i} 75 перцентилей {dm.report(75, first_time, second_time)},')
            print(f'функция {i} 95 перцентилей {dm.report(95, first_time, second_time)}')
            print(f'функция {i} 99 перцентилей {dm.report(99, first_time, second_time)}')


if __name__ == "__main__":
    migrate()
    main("access.log", "main.db")
    report()
