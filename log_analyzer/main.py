import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import time

file = str(input("из какого лога идет запись в бд?"))
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
            with Parser_data_manager(db,file) as dm:
                count = 0
                cur_count = 0
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        dm.false_insert_val(line)
                        continue
                    end = time.process_time()
                    dm.insert_val(row)
                    count += 1
                dm.commit_pdm()
        with open('report.txt', 'w') as report :
            report.write(f'времени заняло {end}, для 50% {dm.report(50)}, для 75% {dm.report(75)}, для 95% {dm.report(95)}, для 99% {dm.report(99)}')
    except FileNotFoundError:
        print("file 'access.log' not found")
    except Exception as e:
        print(repr(e))

if __name__ == "__main__":
    migrate()
    main()
