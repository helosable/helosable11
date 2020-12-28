import sqlite3
import ijson 
import parser_data_manager as dm



data_manager = dm.Parser_data_manager("main.db")
def json_still_valid(js):
    try:
        parse=next(ijson.items(js,"",multiple_values=True))
    except ijson.common.IncompleteJSONError:
        return False
    return parse


with open("access.log","r") as myfile:
    for line in myfile:
        row = json_still_valid(line)
        if row==False:
            print (line)
            data_manager.false_insert_val()
            continue
        data_manager.insert_val(row)
        

            
        