import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager


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
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        print(line)
                        dm.false_insert_val()
                    dm.insert_val(row)

    except FileNotFoundError:
        print("file not found")
    except Exception as e:
        print(repr(e))

if __name__ == "__main__":
    migrate()
    main()
