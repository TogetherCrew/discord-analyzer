from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from tc_analyzer_lib.metrics.utils import Platform
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestPlatformUtilsFetchResources(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.client["Core"].drop_collection("platforms")

    def test_get_period_empty_platform(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_utils = Platform(platform_id)

        with self.assertRaises(AttributeError):
            _ = platform_utils.get_platform_resources()

    def test_get_period_single_platform(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                    "resources": ["channel_0", "channel_1", "channel_2"],
                },
                "period": datetime(2024, 1, 1),
            }
        )

        platform_obj = Platform(platform_id)

        resources = platform_obj.get_platform_resources()
        self.assertIsInstance(resources, list)
        self.assertEqual(resources, ["channel_0", "channel_1", "channel_2"])

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
                        "resources": ["channel_0", "channel_1", "channel_2"],
                    },
                    "period": datetime(2024, 1, 1),
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "resources": ["channel_A", "channel_B", "channel_C"],
                    },
                    "period": datetime(2024, 1, 2),
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "resources": ["channel_0", "channel_1", "channel_2"],
                    },
                    "period": datetime(2024, 1, 3),
                },
            ]
        )

        platform_obj = Platform(platform_id2)

        resources = platform_obj.get_platform_resources()
        self.assertIsInstance(resources, list)
        self.assertEqual(resources, ["channel_A", "channel_B", "channel_C"])

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
                        "resources": ["channel_0", "channel_1", "channel_2"],
                    },
                    "period": datetime(2024, 1, 1),
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "resources": ["channel_0", "channel_1", "channel_2"],
                    },
                    "period": datetime(2024, 1, 2),
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                        "resources": ["channel_0", "channel_1", "channel_2"],
                    },
                    "period": datetime(2024, 1, 3),
                },
            ]
        )

        platform_obj = Platform(platform_id4)

        with self.assertRaises(AttributeError):
            _ = platform_obj.get_platform_resources()
