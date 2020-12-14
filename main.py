import sqlite3
import ijson 
import parser_data_manager as dm



data_manager = dm.Parser_data_manager("main.db")
def json_still_valid(js):
    parse=ijson.items(js,"",multiple_values=True)
    return parse


with open("access.log","r") as myfile:
    for line in myfile:
        row = json_still_valid(line)
        try:
            next_row = next(row)
        except ijson.common.IncompleteJSONError:
            print (line)
            data_manager.false_insert()
            continue

        str_row = str(next_row)
        hashed = data_manager.hashing(str_row)
        if data_manager.compare(hashed) == None:
            data_manager.inserting(next_row)
            
        
