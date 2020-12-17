import unittest
import parser_data_manager


data_manager=parser_data_manager.Parser_data_manager("main.db")

class main(unittest.TestCase):
    def test_work(self,val):
        return self.assertEqual(data_manager.hash_val(val),data_manager.hash_val(val))

test=main()
print(test.test_work("llll"))
