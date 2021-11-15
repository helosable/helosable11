
def sql_query(table_name):
    steps = f"CREATE TABLE IF NOT EXISTS {table_name}"+"""(id INTEGER ,
        time DATETIME,
        remote_addr VARCHAR(40),
        remote_user VARCHAR(40),
        body_bytes_sent VARCHAR(40),
        request_time VARCHAR(40),
        status VARCHAR(40),
        request VARCHAR(40),
        request_method VARCHAR(40),
        http_referrer VARCHAR(40),
        http_user_agent VARCHAR(40),
        proxy_host VARCHAR(40),
        row_hash VARCHAR(35),
        file_name VARCHAR(40)
        ) ENGINE=MergeTree ORDER BY id PRIMARY KEY id """
    return steps
