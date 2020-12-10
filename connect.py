import sqlite3
import ijson 


class db:
    def connect(self):
        cnx= sqlite3.connect("main.db")
        cur=cnx.cursor()
        return cur,cnx   


    def json_still_valid(self,js):
        try:
            parse=ijson.items(js,"",multiple_values=True)
        except ijson.common.IncompleteJSONError:
            return False
        except ijson.JSONError:
            return False
        return parse

    def false_insert(self):
        cur,cnx=connect()
        cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
        cnx.commit()


    def inserting(self,time,remote_addr,remote_user,body_bytes_sent,request_time,status,request,request_method,http_referrer,http_user_agent,proxy_host):
        cur,cnx=connect()
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
