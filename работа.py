import ijson
import sqlite3


new_db=open("new_db.db","w")
cnx= sqlite3.connect("new_db.db")
cur=cnx.cursor()
cur.execute("""CREATE TABLE my_table (id VARCHAR PRIMARY KEY,
time VARCHAR, 
remote_addr VARCHAR, 
remote_user VARCHAR,  
body_bytes_sent VARCHAR, 
request_time VARCHAR, 
status VARCHAR, 
request VARCHAR,
request_method VARCHAR,
http_referrer VARCHAR,
http_user_agent VARCHAR, 
proxy_host VARCHAR)""")


loop=0
arr=[]
id1=1
count=0
check_arr=[]

my_file=ijson.basic_parse(open ('access.log','r', encoding='utf-8'),multiple_values=True)
try:
    for line,event in my_file:
        my_list=[line,event]
        if my_list[0]=='string':
            arr.append(event)
            if len(arr)==11:
                arr.insert(0,str(id1))
                for arr1 in check_arr:
                    if arr[1:]==check_arr :
                        pass
                cur.execute("""INSERT INTO my_table (id ,
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
                            proxy_host) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",(arr))
                check_arr.append(arr)
                arr=[]
                loop+=1
                id1+=1
                if loop == 10000:  
                    loop = 0 
                    cnx.commit()
except ijson.common.IncompleteJSONError as e :
    cnx.commit()  
    print (arr)
    print (e)
    
        

cnx.commit()
