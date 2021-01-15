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


def main(file="access.log", db="main.db"):
    try:
        with open(file, "r") as myfile:
            with Parser_data_manager(db) as dm:
                count = 0
                cur_count = 0
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    if count == 100000:
                        cur_count = cur_count + count
                        count = 0
                        end = time.process_time()
                        print(f"прошло {int(end)} секунд, было сделано {cur_count} коммитов с начала выполнения программы")
                    dm.insert_val(row)
                    count += 1
                dm.commit_pdm()

    except FileNotFoundError:
        print("file 'access.log' not found")
    except Exception as e:
        print(repr(e))

if __name__ == "__main__":
    migrate()
    main()
