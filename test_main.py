import unittest
import parser_data_manager
import ijson


data_manager=parser_data_manager.Parser_data_manager("main.db")


class main(unittest.TestCase):
    def test_hash(self):
        assert(data_manager.false_insert_val())
        with open("access.log","r") as myfile:
            for line in myfile:
                row=next(ijson.items(line,"",multiple_values=True))
                assert(data_manager.insert_val(row))

test=main()
print(test.test_hash())
