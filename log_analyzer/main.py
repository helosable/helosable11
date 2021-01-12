import sys
import ijson
from yoyo import read_migrations, get_backend
from models import parser_data_manager as dm

import importlib
from config import CONFIG
configuration = importlib.import_module(CONFIG)


def json_still_valid(js):
    return ijson.items(js, "", multiple_values=True)


def main():
    fname = sys.argv[1]

    backend = get_backend(configuration.DBNAME)
    migrations = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))

    try:
        with open(fname, "r") as myfile:
            with dm.Parser_data_manager(configuration.DBNAME) as data_manager:
                for line in myfile:
                    row = json_still_valid(line)
                    try:
                        next_row = next(row)
                        data_manager.insert_val(next_row)
                    except ijson.common.IncompleteJSONError:
                        print(line)
                        data_manager.false_insert_val()

    except FileNotFoundError:
        print("Input file not found")
        sys.exit(1)
    except Exception as e:
        print("DB error {0}".format(repr(e)))
        sys.exit(1)


if __name__ == "__main__":
    main()
