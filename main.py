import ijson
import parser_data_manager as pdm
from yoyo import read_migrations, get_backend


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


def main():
    try:
        with open("access.log", "r") as myfile:
            with pdm.Parser_data_manager("main.db") as dm:
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
