from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from tc_analyzer_lib.utils.get_guild_utils import get_platform_community_owner
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestGetGuildOwner(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.client["Core"].drop_collection("platforms")
        self.client["Core"].drop_collection("users")
        self.platform_id = "515151515151515151515151"
        self.community_id = "aabbccddeeff001122334455"
        self.guild_id = "1234"
        self.client.drop_database(self.guild_id)
        self.client.drop_database(self.platform_id)

    def tearDown(self) -> None:
        self.client["Core"].drop_collection("platforms")
        self.client["Core"].drop_collection("users")
        self.client.drop_database(self.guild_id)
        self.client.drop_database(self.platform_id)

    def test_no_platform_available(self):
        with self.assertRaises(AttributeError):
            _ = get_platform_community_owner(self.platform_id)

    def test_no_community_available(self):
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "name": "discord",
                "metadata": {
                    "id": self.guild_id,
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "resources": ["1020707129214111827"],
                    "window": {"period_size": 7, "step_size": 1},
                    "action": {"some_Values": 1},
                    "period": datetime.now() - timedelta(days=30),
                },
                "community": ObjectId(self.community_id),
                "disconnectedAt": None,
                "connectedAt": (datetime.now() - timedelta(days=40)),
                "isInProgress": True,
                "createdAt": datetime(2023, 11, 1),
                "updatedAt": datetime(2023, 11, 1),
            }
        )
        with self.assertRaises(AttributeError):
            _ = get_platform_community_owner(self.platform_id)

    def test_single_platform(self):
        expected_owner_discord_id = "1234567890"

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "name": "discord",
                "metadata": {
                    "id": self.guild_id,
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "resources": ["1020707129214111827"],
                    "window": {"period_size": 7, "step_size": 1},
                    "action": {"some_Values": 1},
                    "period": datetime.now() - timedelta(days=30),
                },
                "community": ObjectId(self.community_id),
                "disconnectedAt": None,
                "connectedAt": (datetime.now() - timedelta(days=40)),
                "isInProgress": True,
                "createdAt": datetime(2023, 11, 1),
                "updatedAt": datetime(2023, 11, 1),
            }
        )
        self.client["Core"]["users"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "discordId": expected_owner_discord_id,
                "communities": [ObjectId(self.community_id)],
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
                "tcaAt": datetime(2023, 12, 2),
            }
        )

        owner = get_platform_community_owner(platform_id=self.platform_id)

        self.assertEqual(
            owner,
            expected_owner_discord_id,
        )

    def test_multiple_platforms(self):
        expected_owner_discord_id = "1234567890"

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "name": "discord",
                "metadata": {
                    "id": self.guild_id,
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "resources": ["1020707129214111827"],
                    "window": {"period_size": 7, "step_size": 1},
                    "action": {"some_Values": 1},
                    "period": datetime.now() - timedelta(days=30),
                },
                "community": ObjectId(self.community_id),
                "disconnectedAt": None,
                "connectedAt": (datetime.now() - timedelta(days=40)),
                "isInProgress": True,
                "createdAt": datetime(2023, 11, 1),
                "updatedAt": datetime(2023, 11, 1),
            }
        )
        self.client["Core"]["users"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "discordId": expected_owner_discord_id,
                "communities": [ObjectId(self.community_id)],
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
                "tcaAt": datetime(2023, 12, 2),
            }
        )

        owner = get_platform_community_owner(platform_id=self.platform_id)

        self.assertEqual(owner, expected_owner_discord_id)

    def test_multiple_platforms_available(self):
        expected_owner_discord_id = "1234567891"
        platform_id2 = "515151515151515151515152"
        platform_id3 = "515151515151515151515153"

        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId(self.platform_id),
                    "name": "discord",
                    "metadata": {
                        "id": self.guild_id,
                        "icon": "111111111111111111111111",
                        "name": "A guild",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 7, "step_size": 1},
                        "action": {"some_Values": 1},
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId(self.community_id),
                    "disconnectedAt": None,
                    "connectedAt": (datetime.now() - timedelta(days=40)),
                    "isInProgress": True,
                    "createdAt": datetime(2023, 11, 1),
                    "updatedAt": datetime(2023, 11, 1),
                },
                {
                    "_id": ObjectId(platform_id2),
                    "name": "discord",
                    "metadata": {
                        "id": self.guild_id,
                        "icon": "111111111111111111111111",
                        "name": "A guild",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 7, "step_size": 1},
                        "action": {"some_Values": 1},
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId("aabbccddeeff001122334456"),
                    "disconnectedAt": None,
                    "connectedAt": (datetime.now() - timedelta(days=40)),
                    "isInProgress": True,
                    "createdAt": datetime(2023, 11, 1),
                    "updatedAt": datetime(2023, 11, 1),
                },
                {
                    "_id": ObjectId(platform_id3),
                    "name": "discord",
                    "metadata": {
                        "id": self.guild_id,
                        "icon": "111111111111111111111111",
                        "name": "A guild",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 7, "step_size": 1},
                        "action": {"some_Values": 1},
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId("aabbccddeeff001122334457"),
                    "disconnectedAt": None,
                    "connectedAt": (datetime.now() - timedelta(days=40)),
                    "isInProgress": True,
                    "createdAt": datetime(2023, 11, 1),
                    "updatedAt": datetime(2023, 11, 1),
                },
            ]
        )
        self.client["Core"]["users"].insert_one(
            {
                "_id": ObjectId(self.platform_id),
                "discordId": expected_owner_discord_id,
                "communities": [ObjectId(self.community_id)],
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
                "tcaAt": datetime(2023, 12, 2),
            }
        )

        owner = get_platform_community_owner(platform_id=self.platform_id)

        self.assertEqual(owner, expected_owner_discord_id)
