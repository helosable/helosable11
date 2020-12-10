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
        db.inserting(next(row))
        loop1+=1
        if loop1==100000:
            loop1=0
            cnx.commit()
        cnx.commit()



if __name__ == "__main__":
    db.inserting()
