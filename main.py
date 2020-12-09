import sqlite3
import json 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()


arr=[]
loop1=0

def json_still_valid(js):
    try:
        parse = list(ijson.basic_parse(js,multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False
    return parse

with open("access.log","r") as myfile:
    for line in myfile:
        row = json_still_valid(line)
        if row == False:
            print (line)
            cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
            cnx.commit()
            continue
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
        loop1+=1
        if loop1==100000:
            loop1=0
            cnx.commit()
    cnx.commit()
   
