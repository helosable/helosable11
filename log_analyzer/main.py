import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
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
    second_migration = read_migrations("./second_migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))
    with sqlite3.connect('main.db') as cnx:
        cur = cnx.cursor()
        p = cur.execute("SELECT * FROM my_table")
        p = cur.fetchall()
        for i in p :
            cur.execute("""INSERT OR IGNORE INTO new_table (
                    time,
                    remote_addr,
                    remote_user,
                    body_bytes_sent,
                    request_time,
                    status,
                    request,
                    request_method,
                    http_referrer,
                    http_user_agent,
                    proxy_host,
                    row_hash,
                    file_name) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    i[1][:10] + ' ' + i[1][11:19],
                    i[2], i[3], i[4], i[5], i[6],
                    i[7], i[8], i[9], i[10], i[11], i[12], i[13],   
                    ))
        cnx.commit()
    with backend.lock():
        backend.apply_migrations(backend.to_apply(second_migration))


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

def report(first_time='2020-10-27 14:45:42', second_time='2020-10-27 14:45:43'):
    with sqlite3.connect('main.db') as cnx:
        cur = cnx.cursor()
        time_first = f"{first_time}"
        time_second = f"{second_time}"
        per_list = [50, 75, 95, 99]
        time_list = []
        func_name1 = cur.execute('SELECT request FROM my_table GROUP BY request ')
        func_name1 = cur.fetchall()
        # func_name = list(cur.execute(f"SELECT request FROM my_table WHERE time = substr('2020-10-27',1,4)||'-'||substr('2020-10-27',6,7)"))
        func_name = list(cur.execute(f"SELECT request FROM my_table WHERE time BETWEEN datetime('2020-10-27 14:45:42') AND datetime('2020-10-27 14:45:43') GROUP BY request "))
        for func in func_name:
            func = func[0]
            if func == 'error':
                continue
            time = cur.execute(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
            time = cur.fetchall()
            new_time = []
            new_per_list = []
            new_per_list.append(func)
            for i in time:
                new_time.append(float(i[0]))
            for i in per_list:
                new_per_list.append(float(numpy.percentile(new_time, i)))
            time_list.append(new_per_list)
        print()
    return time_list


if __name__ == "__main__":
    migrate()
    main("main.db", 'tests/resources/access_mini_false.log')
    report()
