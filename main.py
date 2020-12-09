import sqlite3
import ijson 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()



loop1=0

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
        time=row[2][1]
        remote_addr=row[4][1]
        remote_user=row[6][1]
        body_bytes_sent=row[8][1]
        request_time=row[10][1]
        status=row[12][1]
        request=row[14][1]
        request_method=row[16][1]
        http_referrer=row[18][1]
        http_user_agent=row[20][1]
        proxy_host=row[22][1]
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
   
