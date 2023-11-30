from datetime import datetime
from unittest import TestCase

from bson.objectid import ObjectId
from utils.get_guild_id import get_guild_id
from utils.get_mongo_client import MongoSingleton


class TestGetGuildId(TestCase):
    def test_get_guild_id_avalable_guild(self):
        client = MongoSingleton.get_instance().client
        platform_id = ObjectId("515151515151515151515151")

        client.drop_database("Core")
        client["Core"]["Platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "id": "999888877766655",
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "selectedChannels": [
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

        guild_id = get_guild_id(platform_id)
        self.assertEqual(guild_id, "999888877766655")

    def test_guild_id_no_document_raise_error(self):
        client = MongoSingleton.get_instance().client
        platform_id = ObjectId("515151515151515151515151")

        client.drop_database("Core")

        with self.assertRaises(AttributeError):
            get_guild_id(platform_id)
