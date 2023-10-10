import os
import unittest
from dotenv import load_dotenv

from automation.utils.model import AutomationDB
from utils.get_mongo_client import MongoSingleton


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
            },
        ]

        client[db_name][collection_name].insert_many(automations_dict)

        automation_db = AutomationDB()

        automations = automation_db.load_from_db(guild_id="123")

        expected_automations_guild_123 = [
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
            },
        ]
        self.assertEqual(len(automations), 2)
        for at in automations:
            at_dict = at.to_dict()

            self.assertIn(at_dict, expected_automations_guild_123)

        automations = automation_db.load_from_db(guild_id="124")
        expected_automations_guild_124 = [
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
            }
        ]

        self.assertEqual(len(automations), 1)
        for at in automations:
            at_dict = at.to_dict()

            self.assertEqual(at_dict, expected_automations_guild_124[0])
