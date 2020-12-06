import sqlite3
import ijson 


cnx= sqlite3.connect("main.db")
cur=cnx.cursor()


arr=[]
loop=0
id1=1

try:
    with open("access.log","r") as my_file:
        row = ijson.basic_parse(my_file,multiple_values=True)
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
                    loop+=1
                    id1+=1
                    if loop == 100000:  
                        loop = 0 
                        cnx.commit()
except ijson.common.IncompleteJSONError as e :
    cnx.commit()
    my_file.close()   
    print (e)
        

cnx.commit()
my_file.close()