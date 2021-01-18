import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import time


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


def main(file_name, db_name):
    try:
        with open(file_name, "r") as myfile:
            with Parser_data_manager(db_name) as dm:
                count = 0
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    if count == 100000:
                        count = 0
                        end = time.process_time()
                        print(f"прошло {int(end)} секунд с начала выполнения программы")
                    dm.insert_val(row)
                    count += 1

    except FileNotFoundError:
        print("file not found")
    except Exception as e:
        print(repr(e))

if __name__ == "__main__":
    migrate()
    main("access.log", "main.db")
