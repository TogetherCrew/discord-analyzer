import os
import unittest
from datetime import datetime, timedelta

from dotenv import load_dotenv
from tc_analyzer_lib.automation.utils.model import AutomationDB
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestAutomationDBLoadFromDB(unittest.TestCase):
    def test_load_from_db_no_results(self):
        instance = MongoSingleton.get_instance()
        client = instance.get_client()

        load_dotenv()
        db_name = os.getenv("AUTOMATION_DB_NAME")
        collection_name = os.getenv("AUTOMATION_DB_COLLECTION")

        client[db_name].drop_collection(collection_name)

        automation_db = AutomationDB()
        automations = automation_db.load_from_db(guild_id="123")
        self.assertEqual(automations, [])

    def test_load_from_db(self):
        instance = MongoSingleton.get_instance()
        client = instance.get_client()

        load_dotenv()
        db_name = os.getenv("AUTOMATION_DB_NAME")
        collection_name = os.getenv("AUTOMATION_DB_COLLECTION")

        client[db_name].drop_collection(collection_name)
        past_two_day_created_at = datetime.now() - timedelta(days=2)
        yesterday_created_at = datetime.now() - timedelta(days=1)
        today_created_at = datetime.now() - timedelta(days=0)

        automations_dict = [
            {
                "guildId": "123",
                "triggers": [
                    {"options": {"category": "all_new_disengaged"}, "enabled": True},
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ],
                "actions": [
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                ],
                "report": {
                    "recipientIds": ["111"],
                    "template": "hey {{username}}, this is a report!",
                    "options": {},
                    "enabled": True,
                },
                "enabled": True,
                "createdAt": past_two_day_created_at,
                "updatedAt": past_two_day_created_at,
                "id": "dsajhf2390j0wadjc",
            },
            {
                "guildId": "124",
                "triggers": [
                    {"options": {"category": "all_new_disengaged"}, "enabled": True},
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ],
                "actions": [
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                ],
                "report": {
                    "recipientIds": ["111"],
                    "template": "hey {{username}}, this is a report!",
                    "options": {},
                    "enabled": True,
                },
                "enabled": True,
                "createdAt": yesterday_created_at,
                "updatedAt": yesterday_created_at,
                "id": "328qujmajdsoiwur",
            },
            {
                "guildId": "123",
                "triggers": [
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ],
                "actions": [
                    {
                        "template": "hello {{username}}!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hi {{username}}!",
                        "options": {},
                        "enabled": True,
                    },
                ],
                "report": {
                    "recipientIds": ["111", "113"],
                    "template": "hey {{username}}, this is a report!",
                    "options": {},
                    "enabled": True,
                },
                "enabled": True,
                "createdAt": today_created_at,
                "updatedAt": today_created_at,
                "id": "uahfoiewrj8979832",
            },
        ]

        client[db_name][collection_name].insert_many(automations_dict)

        automation_db = AutomationDB()

        automations = automation_db.load_from_db(guild_id="123")

        self.assertEqual(len(automations), 2)
        for at in automations:
            at_dict = at.to_dict()

            self.assertEqual(at_dict["guildId"], "123")

            assert (
                at_dict["triggers"]
                == [
                    {"options": {"category": "all_new_disengaged"}, "enabled": True},
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ]
            ) or (
                at_dict["triggers"]
                == [
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ]
            )

            assert (
                at_dict["actions"]
                == [
                    {
                        "template": "hello {{username}}!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hi {{username}}!",
                        "options": {},
                        "enabled": True,
                    },
                ]
            ) or (
                at_dict["actions"]
                == [
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                ]
            )

            self.assertIn(
                at_dict["report"],
                [
                    {
                        "recipientIds": ["111", "113"],
                        "template": "hey {{username}}, this is a report!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "recipientIds": ["111"],
                        "template": "hey {{username}}, this is a report!",
                        "options": {},
                        "enabled": True,
                    },
                ],
            )
            self.assertEqual(at_dict["enabled"], True)
            self.assertIn(
                at_dict["createdAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                [
                    past_two_day_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                    today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                ],
            )
            self.assertIn(
                at_dict["updatedAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                [
                    past_two_day_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                    today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                ],
            )
            self.assertIn(at_dict["id"], ["dsajhf2390j0wadjc", "uahfoiewrj8979832"])
        automations = automation_db.load_from_db(guild_id="124")

        self.assertEqual(len(automations), 1)
        for at in automations:
            at_dict = at.to_dict()

            self.assertEqual(at_dict["guildId"], "124")
            self.assertEqual(
                at_dict["triggers"],
                [
                    {"options": {"category": "all_new_disengaged"}, "enabled": True},
                    {"options": {"category": "all_new_active"}, "enabled": True},
                ],
            )
            self.assertEqual(
                at_dict["actions"],
                [
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                    {
                        "template": "hey {{username}}! please get back to us!",
                        "options": {},
                        "enabled": True,
                    },
                ],
            )
            self.assertEqual(
                at_dict["report"],
                {
                    "recipientIds": ["111"],
                    "template": "hey {{username}}, this is a report!",
                    "options": {},
                    "enabled": True,
                },
            )
            self.assertEqual(at_dict["enabled"], True)
            self.assertEqual(
                at_dict["createdAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                yesterday_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            )
            self.assertEqual(at_dict["id"], "328qujmajdsoiwur")
