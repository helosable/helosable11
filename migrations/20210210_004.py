from yoyo import step

_depends_ = {"20201226_002.py", "20201206_000_initial.py", "20201206_001.py", "20210116_003.py"}

steps = [
    step("""CREATE TABLE IF NOT EXISTS new_table (id INTEGER AUTO_INCREMENT,
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
    file_name VARCHAR,
    row_hash VARCHAR(35),
    PRIMARY KEY(id))"""),
    step("CREATE UNIQUE INDEX hash_unique_index_1 ON new_table(row_hash)")
]
