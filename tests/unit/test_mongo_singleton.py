import unittest

from pymongo import MongoClient
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestMongoSingleton(unittest.TestCase):
    def test_singleton_instance(self):
        mongo_singleton_1 = MongoSingleton.get_instance()
        mongo_singleton_2 = MongoSingleton.get_instance()
        self.assertEqual(mongo_singleton_1, mongo_singleton_2)

    def test_mongo_client(self):
        mongo_singleton = MongoSingleton.get_instance()
        mongo_client = mongo_singleton.get_client()
        self.assertIsInstance(mongo_client, MongoClient)
