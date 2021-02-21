import ijson
from yoyo import read_migrations, get_backend
from models.parser_data_manager import Parser_data_manager
import sys
import sqlite3
import numpy


try:
    first_time = sys.argv[1]
    second_time = sys.argv[2]
except IndexError:
    pass


def json_still_valid(js):
    try:
        return next(ijson.items(js, "", multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False


def migrate():
    backend = get_backend("sqlite:///main.db")
    migration = read_migrations("./migrations")
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migration))
    with sqlite3.connect('main.db') as cnx:
        cur = cnx.cursor()
        p = cur.execute("SELECT * FROM my_table")
        p = cur.fetchall()
        try:
            cur.execute("""CREATE TABLE new_table (id INTEGER AUTO_INCREMENT,
        time date,
        remote_addr VARCHAR,
        remote_user VARCHAR,
        body_bytes_sent VARCHAR,
        request_time VARCHAR,
        status VARCHAR,
        request VARCHAR,
        request_method VARCHAR,
        http_referrer VARCHAR,
        http_user_agent VARCHAR,
        proxy_host VARCHAR,
        row_hash VARCHAR(35),
        file_name VARCHAR,
        PRIMARY KEY(id))""")
        except sqlite3.OperationalError:
            pass

        try:
            cur.execute("CREATE UNIQUE INDEX hash_unique_index_1 ON new_table(row_hash)")
        except sqlite3.OperationalError:
            pass
        with Parser_data_manager('main.db') as dm:

            for i in p:
                obj = {'time': f'{i[0]}', 'remote_addr': f'{i[1]}', 'remote_user': f'{i[2]}',
                    'body_bytes_sent': f'{i[3]}',
                    'request_time': f'{i[4]}', 'status': f'{i[5]}', 'request' : f'{i[6]}', 
                    'request_method': f'{i[7]}', 'http_referrer': f'{i[8]}',
                    'http_user_agent': f'{i[9]}', 'proxy_host': f'{i[10]}', 
                    'file_name': f'{i[12]}'}
                if len(obj['time']) > 20:
                    dm.insert_val(obj, obj['file_name'])
        try:
            cur.execute("DROP TABLE my_table")
        except sqlite3.OperationalError :
            pass
        try:
            cur.execute("ALTER TABLE new_table RENAME TO my_table")
        except sqlite3.OperationalError :
            pass
       


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
        pi = cur.execute("SELECT * FROM my_table")
        pi = cur.fetchall()
        func_name1 = cur.execute('SELECT request FROM my_table GROUP BY request ')
        func_name1 = cur.fetchall()
        c_1 = 0
        func_name = list(cur.execute(f"SELECT request FROM my_table WHERE time BETWEEN datetime('{time_first}') AND datetime('{time_second}') GROUP BY request "))
        with open('templates/report.csv', 'a') as myfile:
            myfile.write("func_name, 50 per, 75 per, 95 per, 99 per \n")
            for func in func_name:
                func = func[0]
                if func == 'error':
                    continue
                time = cur.execute(f'SELECT request_time FROM my_table WHERE request = "{func}" ')
                time = cur.fetchall()
                new_time = []
                new_per_list = []
                new_per_list.append(func)
                myfile.write(f'{func}, ')
                for i in time:
                    new_time.append(float(i[0]))
                for i in per_list:
                    new_per_list.append(float(numpy.percentile(new_time, i)))
                    myfile.write(f"{float(numpy.percentile(new_time, i))}, ")
                    c_1 += 1
                    if c_1 == 4:
                        myfile.write(f"{float(numpy.percentile(new_time, i))},\n")
                        c_1 = 0
                    
                time_list.append(new_per_list)
    return time_list


if __name__ == "__main__":
    migrate()
    main("main.db", 'tests/resources/access_mini_false.log')
    print(report(first_time, second_time))
