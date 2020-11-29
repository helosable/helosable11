import sqlite3
import json 

new_db=open("new_db.db","w")
cnx= sqlite3.connect("new_db.db")
cur=cnx.cursor()
cur.execute("""CREATE TABLE my_table (time VARCHAR, 
remote_addr VARCHAR, 
remote_user VARCHAR,  
body_bytes_sent VARCHAR, 
request_time VARCHAR, 
status VARCHAR, 
request VARCHAR,
request_method VARCHAR,
http_referrer VARCHAR,
http_user_agent VARCHAR, 
proxy_host VARCHAR )""")

count = 1
loop_count = 0
my_file =open("access.log","r")
for line in my_file:
    row = json.loads(line)
    time=row['time']
    remote_addr=row['remote_addr']
    remote_user=row['remote_user']
    body_bytes_sent=row['body_bytes_sent']
    request_time=row['request_time']
    status=row['status']
    request=row['request']
    request_method=row['request_method']
    http_referrer=row['http_referrer']
    http_user_agent=row['http_user_agent']
    proxy_host=row['proxy_host']
    cur.execute("""INSERT INTO my_table (time, 
    remote_addr, 
    remote_user,  
    body_bytes_sent, 
    request_time, 
    status, 
    request,
    request_method,
    http_referrer,
    http_user_agent, 
    proxy_host) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",(
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
    proxy_host
    ))
    loop_count += 1
    count += 1
    print (f"внедряем {count} строчку , осталось:{3939161-count} , выполнено {round(count/3939161*100)} процентов")
    if loop_count == 100000:
        cnx.commit()
        loop_count= 0
        


cnx.commit()
cur.close()
my_file.close()
