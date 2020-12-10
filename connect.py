import sqlite3
import ijson 


class db:
    def connect(self):
        cnx= sqlite3.connect("main.db")
        cur=cnx.cursor()
        return cur , cnx
        


    def json_still_valid(self,js):
        try:
            parse=ijson.items(js,"",multiple_values=True)
        except ijson.common.IncompleteJSONError:
            return False
        except ijson.JSONError:
            return False
        return parse

    def inserting(self):
        with open("access.log","r") as myfile:
            loop1=0
            cur,cnx=connect()
            for line in myfile:
                row = json_still_valid(line)
                if row == False:
                    print (line)
                    cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
                    cnx.commit()
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
