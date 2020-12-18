import unittest
import parser_data_manager
import ijson
import hashlib


class main(unittest.TestCase):
    def test_hash(self):
        hash1="5eb63bbbe01eeed093cb22bb8f5acdc3"
        assert hash1==parser_data_manager.Parser_data_manager.hash_val("hello world")

if __name__ == "__main__":
      unittest.main()


