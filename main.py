import sqlite3
import ijson 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()


arr=[]
loop1=0

def json_still_valid(js):
    try:
        parse = list(ijson.basic_parse(js,multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False
    except ijson.JSONError:
        return False
    return parse

with open("access.log","r") as my_file:
    for loop in my_file:
        row = json_still_valid(loop)
        if type(row)==list:  
            for line,event in row:
                my_list=[line,event]
                if my_list[0]=='string':
                    arr.append(event)
                if len(arr)==11:
                    cur.execute("""INSERT INTO my_table (
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
                                proxy_host) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",(arr))
                    arr=[]
                    loop1+=1
                    if loop1 == 100000:  
                        loop1 = 0 
                        cnx.commit()
        elif row==False:
            print(loop)
            cur.execute("""INSERT INTO my_table (time) VALUES ('не получилось')""")
            cnx.commit()
    cnx.commit()
   
