import unittest
import parser_data_manager
import ijson
import hashlib


test_hash=hashlib.sha256(str("pp").encode("utf-8")).hexdigest()
data_manager=parser_data_manager.Parser_data_manager("main.db")


class main(unittest.TestCase):
    def test_hash(self):
        assert(data_manager.false_insert_val())
        with open("access.log","r") as myfile:
            for line in myfile:
                row=next(ijson.items(line,"",multiple_values=True))
                test_hash=hashlib.sha256(str(row).encode("utf-8")).hexdigest()
                assert test_hash!=data_manager.hash_val(row)
                assert row == data_manager.insert_val(row)

test=main()
print(test.test_hash())
