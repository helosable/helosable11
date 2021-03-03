from yoyo import step

_depends_ = {"20201226_002.py", "20201206_000_initial.py", "20201206_001.py", "20210116_003.py"}

steps = [
    step("""CREATE TABLE new_table IF NOT EXISTS (id INTEGER AUTO_INCREMENT,
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
    PRIMARY KEY(id))"""),
    step("CREATE UNIQUE INDEX hash_unique_index_1 ON new_table(row_hash)"),
    step("""DECLARE @time_var AS VARCHAR 
    DECLATE @all_time AS VARCHAR
    SET @all_time = SELECT time FROM my_table
    IF LEN(all_time) > 20 
    SET @time_var = CONCAT(SUBSTR(time 1, 10) || ' ' ||  SUBSTR(time 12,20)
    ELSE 
    SET @time_var = all_time 
    INSERT OR IGNORE INTO new_table (
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
            VALUES  (@time_var,
            SELECT remote_addr,
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
            file_name 
            FROM my_table)"""),
            step("DROP TABLE my_table"),
            step("ALTER TABLE new_table RENAME TO my_table")
]