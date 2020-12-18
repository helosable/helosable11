import unittest
import parser_data_manager
import ijson
import hashlib


hash1="5eb63bbbe01eeed093cb22bb8f5acdc3"

dm=parser_data_manager.Parser_data_manager()


class main(unittest.TestCase):
    def test_hash(self):
        assert hash1==dm.hash_val("hello_world")

