from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from tc_analyzer_lib.metrics.utils import Platform
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestPlatformUtilsFetchPeriod(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.client["Core"].drop_collection("platforms")

    def test_get_period_empty_platform(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_utils = Platform(platform_id)

        with self.assertRaises(AttributeError):
            _ = platform_utils.get_platform_period()

    def test_get_period_single_platform(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                    "period": datetime(2024, 1, 1),
                },
                "resources": ["channel_0", "channel_1", "channel_2"],
            }
        )

        platform_obj = Platform(platform_id)

        period = platform_obj.get_platform_period()
        self.assertIsInstance(period, datetime)
        self.assertEqual(period, datetime(2024, 1, 1))

    def test_get_period_multiple_platforms(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        platform_id3 = "60d5ec44f9a3c2b6d7e2d11c"

        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId(platform_id),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 1),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 2),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 3),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
            ]
        )

        platform_obj = Platform(platform_id2)

        period = platform_obj.get_platform_period()
        self.assertIsInstance(period, datetime)
        self.assertEqual(period, datetime(2024, 1, 2))

    def test_get_period_irrelevant_multiple_platforms(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        platform_id3 = "60d5ec44f9a3c2b6d7e2d11c"
        platform_id4 = "60d5ec44f9a3c2b6d7e2d11d"

        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId(platform_id),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 1),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 2),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "period": datetime(2024, 1, 3),
                    },
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
            ]
        )

        platform_obj = Platform(platform_id4)

        with self.assertRaises(AttributeError):
            _ = platform_obj.get_platform_period()
