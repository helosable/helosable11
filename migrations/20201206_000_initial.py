from yoyo import step

_depends_ = {}

steps = [
    step("CREATE TABLE my_table (time VARCHAR, remote_addr VARCHAR, remote_user VARCHAR, body_bytes_sent VARCHAR, request_time VARCHAR, status VARCHAR, request VARCHAR, request_method VARCHAR, http_referrer VARCHAR, http_user_agent VARCHAR, proxy_host VARCHAR)")
]