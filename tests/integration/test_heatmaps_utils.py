from datetime import datetime
from unittest import TestCase

from discord_analyzer.analyzer.heatmaps.heatmaps_utils import HeatmapsUtils
from utils.mongo import MongoSingleton


class TestHeatmapsUtils(TestCase):
    def setUp(self) -> None:
        client = MongoSingleton.get_instance().get_client()
        self.platform_id = "1234567890"
        self.database = client[self.platform_id]
        self.database.drop_collection("rawmembers")

        self.utils = HeatmapsUtils(self.platform_id)

    def test_get_users_empty_collection(self):
        users = self.utils.get_users()
        self.assertEqual(list(users), [])

    def test_get_real_users(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        self.database["rawmembers"].insert_many(sample_users)

        users = self.utils.get_users()

        self.assertEqual(list(users), [{"id": 9000}, {"id": 9001}])

    def test_get_bots(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        self.database["rawmembers"].insert_many(sample_users)

        users = self.utils.get_users(is_bot=True)

        self.assertEqual(list(users), [{"id": 9001}, {"id": 9002}])

    def test_get_users_count_empty_data(self):
        count = self.utils.get_users_count()
        self.assertIsInstance(count, int)
        self.assertEqual(count, 0)

    def test_get_users_count_real_users(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        self.database["rawmembers"].insert_many(sample_users)

        count = self.utils.get_users_count()
        self.assertIsInstance(count, int)
        self.assertEqual(count, 2)

    def test_get_users_count_bots(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
            {
                "id": 9003,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 2, 1),
                "options": {},
            },
            {
                "id": 9004,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 2, 3),
                "options": {},
            },
        ]
        self.database["rawmembers"].insert_many(sample_users)

        count = self.utils.get_users_count(is_bot=True)
        self.assertIsInstance(count, int)
        self.assertEqual(count, 3)
