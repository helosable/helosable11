import sqlite3
import ijson 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()



loop1=0
arr=[]

def json_still_valid(js):
    try:
        parse = list(ijson.basic_parse(js,multiple_values=True))
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
        for i in row:
            arr.append(i[1])
        time=arr[arr.index("time")+1]
        remote_addr=arr[arr.index("remote_addr")+1]
        remote_user=arr[arr.index("remote_user")+1]
        body_bytes_sent=arr[arr.index("body_bytes_sent")+1]
        request_time=arr[arr.index("request_time")+1]
        status=arr[arr.index("status")+1]
        request=arr[arr.index("request")+1]
        request_method=arr[arr.index("request_method")+1]
        http_referrer=arr[arr.index("http_referrer")+1]
        http_user_agent=arr[arr.index("http_user_agent")+1]
        proxy_host=arr[arr.index("proxy_host")+1]
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
   
