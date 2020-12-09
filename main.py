import sqlite3
import ijson 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()



loop1=0
arr=[]

def json_still_valid(js):
    try:
        row=list(ijson.items(js,"",multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False
    except ijson.JSONError:
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
        
        time=row[0].get("time")
        remote_addr=row[0].get("remote_addr")
        remote_user=row[0].get("remote_user")
        body_bytes_sent=row[0].get(" body_bytes_sent")
        request_time=row[0].get("request_time")
        status=row[0].get("status")
        request=row[0].get("request")
        request_method=row[0].get("request_method")
        http_referrer=row[0].get("http_referrer")
        http_user_agent=row[0].get("http_user_agent")
        proxy_host=row[0].get("proxy_host")
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
        arr=[]
        loop1+=1
        if loop1==100000:
            loop1=0
            cnx.commit()
    cnx.commit()
   
