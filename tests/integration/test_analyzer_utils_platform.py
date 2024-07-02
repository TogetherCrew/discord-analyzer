from unittest import TestCase

from bson import ObjectId
from tc_analyzer_lib.metrics.utils import Platform
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestAnalyzerUtilsPlatform(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

        self.client["Core"].drop_collection("platforms")

    def test_no_platforms_check_existance(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_obj = Platform(platform_id)

        self.assertFalse(platform_obj.check_existance())

    def test_single_platforms_check_existance(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                },
            }
        )
        platform_obj = Platform(platform_id)

        self.assertTrue(platform_obj.check_existance())

    def test_single_platforms_irrelevant_check_existance(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                },
            }
        )
        # checking for the second platform availability on db
        platform_obj = Platform(platform_id2)
        self.assertFalse(platform_obj.check_existance())

    def test_multiple_platforms_irrelevant_check_existance(self):
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
                    },
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                    },
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "isInProgress": True,
                    },
                },
            ]
        )
        # checking for the fourth platform availability on db
        platform_obj = Platform(platform_id4)
        self.assertFalse(platform_obj.check_existance())

    def test_single_platform_update_isin_progress(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                },
            }
        )

        platform_obj = Platform(platform_id)
        platform_obj.update_isin_progress()

        platform = self.client["Core"]["platforms"].find_one(
            {"_id": ObjectId(platform_id)}
        )

        self.assertFalse(platform["metadata"]["isInProgress"])

    def test_single_platform_unavailable_platform_update_isin_progress(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "isInProgress": True,
                },
            }
        )

        platform_obj = Platform(platform_id2)

        # the platform was not available
        with self.assertRaises(AttributeError):
            platform_obj.update_isin_progress()

    def test_get_community_id_no_platforms(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_obj = Platform(platform_id)

        # no platform was available
        with self.assertRaises(ValueError):
            platform_obj.get_community_id()

    def test_get_community_id_single_platform_available(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        expected_community_id = "77d5ec44f6a3c2b6d7e2d11a"

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "community": ObjectId(expected_community_id),
                "metadata": {
                    "isInProgress": True,
                },
            }
        )
        platform_obj = Platform(platform_id)
        community_id = platform_obj.get_community_id()

        self.assertEqual(expected_community_id, community_id)

    def test_get_community_id_irrelevant_platform_available(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        community_id = "77d5ec44f6a3c2b6d7e2d11a"

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id2),
                "name": "discord",
                "community": ObjectId(community_id),
                "metadata": {
                    "isInProgress": True,
                },
            }
        )
        platform_obj = Platform(platform_id)

        with self.assertRaises(ValueError):
            _ = platform_obj.get_community_id()
