import sqlite3
import ijson 
from connect import db


with open("access.log","r") as myfile:
    loop1=0
    cur,cnx=db.connect()
    for line in myfile:
        row = db.json_still_valid(line)
        if row == False:
            print (line)
            db.false_insert()
            continue            
        for i in row:
            time=i['time']
            remote_addr=i['remote_addr']
            remote_user=i['remote_user']
            body_bytes_sent=i['body_bytes_sent']
            request_time=i['request_time']
            status=i['status']
            request=i['request']
            request_method=i['request_method']
            http_referrer=i['http_referrer']
            http_user_agent=i['http_user_agent']
            proxy_host=i['proxy_host']
        db.inserting(time,remote_addr,remote_user,body_bytes_sent,request_time,status,request,request_method,http_referrer,http_user_agent,proxy_host)
        loop1+=1
        if loop1==100000:
            loop1=0
            cnx.commit()
        cnx.commit()



if __name__ == "__main__":
    db.inserting()
