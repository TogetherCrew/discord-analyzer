from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from tc_analyzer_lib.metrics.utils import Platform
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestAnalyzerUtilsPlatform(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

        self.client["Core"].drop_collection("platforms")

    def test_no_platform(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_obj = Platform(platform_id)

        with self.assertRaises(AttributeError):
            platform_obj.get_platform_analyzer_params()

    def test_single_platform_available(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        guildId = "1234"
        sample_action = {
            "INT_THR": 1,
            "UW_DEG_THR": 1,
            "PAUSED_T_THR": 1,
            "CON_T_THR": 4,
            "CON_O_THR": 3,
            "EDGE_STR_THR": 5,
            "UW_THR_DEG_THR": 5,
            "VITAL_T_THR": 4,
            "VITAL_O_THR": 3,
            "STILL_T_THR": 2,
            "STILL_O_THR": 2,
            "DROP_H_THR": 2,
            "DROP_I_THR": 1,
        }
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId(platform_id),
                "name": "discord",
                "metadata": {
                    "id": guildId,
                    "icon": "111111111111111111111111",
                    "name": "A guild",
                    "resources": ["1020707129214111827"],
                    "window": {"period_size": 7, "step_size": 1},
                    "action": sample_action,
                    "period": datetime.now() - timedelta(days=30),
                },
                "community": ObjectId("aabbccddeeff001122334455"),
                "disconnectedAt": None,
                "connectedAt": (datetime.now() - timedelta(days=40)),
                "isInProgress": True,
                "createdAt": datetime(2023, 11, 1),
                "updatedAt": datetime(2023, 11, 1),
            }
        )
        platform_obj = Platform(platform_id)
        window, action = platform_obj.get_platform_analyzer_params()

        self.assertEqual(window, {"period_size": 7, "step_size": 1})
        self.assertEqual(sample_action, action)

    def test_multiple_platforms_available(self):
        platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        platform_id2 = "60d5ec44f9a3c2b6d7e2d11b"
        platform_id3 = "60d5ec44f9a3c2b6d7e2d11c"

        guildId = "1234"
        guildId2 = "1235"
        guildId3 = "1236"

        sample_action = {
            "INT_THR": 1,
            "UW_DEG_THR": 1,
            "PAUSED_T_THR": 1,
            "CON_T_THR": 4,
            "CON_O_THR": 3,
            "EDGE_STR_THR": 5,
            "UW_THR_DEG_THR": 5,
            "VITAL_T_THR": 4,
            "VITAL_O_THR": 3,
            "STILL_T_THR": 2,
            "STILL_O_THR": 2,
            "DROP_H_THR": 2,
            "DROP_I_THR": 1,
        }

        sample_action2 = {
            "INT_THR": 4,
            "UW_DEG_THR": 5,
            "PAUSED_T_THR": 8,
            "CON_T_THR": 4,
            "CON_O_THR": 3,
            "EDGE_STR_THR": 1,
            "UW_THR_DEG_THR": 5,
            "VITAL_T_THR": 4,
            "VITAL_O_THR": 8,
            "STILL_T_THR": 2,
            "STILL_O_THR": 24,
            "DROP_H_THR": 23,
            "DROP_I_THR": 1,
        }
        sample_action3 = {
            "INT_THR": 1,
            "UW_DEG_THR": 1,
            "PAUSED_T_THR": 1,
            "CON_T_THR": 1,
            "CON_O_THR": 1,
            "EDGE_STR_THR": 1,
            "UW_THR_DEG_THR": 1,
            "VITAL_T_THR": 1,
            "VITAL_O_THR": 1,
            "STILL_T_THR": 1,
            "STILL_O_THR": 14,
            "DROP_H_THR": 13,
            "DROP_I_THR": 1,
        }
        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId(platform_id),
                    "name": "discord",
                    "metadata": {
                        "id": guildId,
                        "icon": "111111111111111111111111",
                        "name": "guild 1",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 7, "step_size": 1},
                        "action": sample_action,
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId("aabbccddeeff001122334455"),
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
                        "id": guildId2,
                        "icon": "111111111111111111111111",
                        "name": "guild 2",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 2, "step_size": 2},
                        "action": sample_action2,
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId("aabbccddeeff001122334455"),
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
                        "id": guildId3,
                        "icon": "111111111111111111111111",
                        "name": "guild 3",
                        "resources": ["1020707129214111827"],
                        "window": {"period_size": 4, "step_size": 3},
                        "action": sample_action3,
                        "period": datetime.now() - timedelta(days=30),
                    },
                    "community": ObjectId("aabbccddeeff001122334455"),
                    "disconnectedAt": None,
                    "connectedAt": (datetime.now() - timedelta(days=40)),
                    "isInProgress": True,
                    "createdAt": datetime(2023, 11, 1),
                    "updatedAt": datetime(2023, 11, 1),
                },
            ]
        )
        platform_obj = Platform(platform_id2)
        window, action = platform_obj.get_platform_analyzer_params()

        self.assertEqual(window, {"period_size": 2, "step_size": 2})
        self.assertEqual(sample_action2, action)
