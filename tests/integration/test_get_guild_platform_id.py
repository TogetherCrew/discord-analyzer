from datetime import datetime
from unittest import TestCase

from bson.objectid import ObjectId
from tc_analyzer_lib.utils.get_guild_utils import get_guild_platform_id
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestGetGuildId(TestCase):
    def test_get_avalable_guild(self):
        client = MongoSingleton.get_instance().client
        platform_id = ObjectId("515151515151515151515151")
        guild_id = "999888877766655"

        client.drop_database("Core")
        client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "id": guild_id,
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "resources": [
                        "11111111",
                        "22222222",
                        "33333333",
                        "44444444",
                        "55555555",
                        "66666666",
                        "77777777",
                    ],
                    "period": datetime(2023, 6, 1),
                },
                "community": ObjectId("aabbccddeeff001122334455"),
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 11, 1),
                "isInProgress": True,
                "createdAt": datetime(2023, 11, 1),
                "updatedAt": datetime(2023, 11, 1),
                "__v": 0,
            }
        )

        platform_id = get_guild_platform_id(guild_id)
        self.assertEqual(platform_id, "515151515151515151515151")

    def test_no_document_raise_error(self):
        client = MongoSingleton.get_instance().client
        guild_id = "999888877766655"

        client.drop_database("Core")

        with self.assertRaises(ValueError):
            get_guild_platform_id(guild_id)
