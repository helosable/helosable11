from yoyo import step

_depends_ = {"20201226_002.py", "20201206_000_initial.py", "20201206_001.py", "20210116_003.py"}

steps = [
    step("""CREATE TABLE new_table (id INTEGER AUTO_INCREMENT,
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
    PRIMARY KEY(id))"""),
    step("ALTER TABLE new_table ADD row_hash VARCHAR(35)"),
    step("ALTER TABLE new_table ADD file_name VARCHAR(35)"),
    step("INSERT INTO new_table SELECT * FROM my_table"),
    step("DROP TABLE my_table"),
    step("ALTER TABLE new_table RENAME TO my_table")
]
