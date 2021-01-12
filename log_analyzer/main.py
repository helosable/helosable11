from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import ijson


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


def main(file="access_mini_false.log", db="main.db"):
    try:
        with open(file, "r") as myfile:
            with Parser_data_manager(db) as dm:
                for line in myfile:
                    row = json_still_valid(line)
                    if not row:
                        row = {"time": "не получилось",
               "remote_addr": "103.42.20.221",
               "remote_user": "03039",
               "body_bytes_sent": "162",
               "request_time": "0.000",
               "status": "301",
               "request": "POST /d4w/api/getNewBookingsLong HTTP/1.1",
               "request_method": "POST",
               "http_referrer": "-",
               "http_user_agent": "SQLAnywhere/16.0.0.2546",
               "proxy_host": "-"}
                        print(line)
                        dm.insert_val(row)
                        continue
                    dm.insert_val(row)

    except FileNotFoundError:
        print("file 'access.log' not found")
    except Exception as e:
        print(repr(e))

if __name__ == "__main__":
    migrate()
    main()
