import os
import unittest
from datetime import datetime, timezone

from dotenv import load_dotenv
from tc_analyzer_lib.automation.utils.interfaces import Automation
from tc_analyzer_lib.automation.utils.model import AutomationDB
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestAutomationDBSaveToDB(unittest.TestCase):
    def test_save_to_db_automation_instance(self):
        instance = MongoSingleton.get_instance()
        client = instance.get_client()

        load_dotenv()
        db_name = os.getenv("AUTOMATION_DB_NAME")
        collection_name = os.getenv("AUTOMATION_DB_COLLECTION")

        client[db_name].drop_collection(collection_name)

        today_created_at = datetime.now(tz=timezone.utc)

        automation_dict = {
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
            "id": "hafujwe09023",
        }

        automation = Automation.from_dict(automation_dict)
        automation_db = AutomationDB()

        automation_db.save_to_db(automation)

        cursor = client[db_name][collection_name].find({"guildId": "123"}, {"_id": 0})

        at_from_db = list(cursor)

        self.assertEqual(len(at_from_db), 1)
        self.assertEqual(at_from_db[0]["guildId"], "123")
        self.assertEqual(
            at_from_db[0]["triggers"],
            [
                {"options": {"category": "all_new_active"}, "enabled": True},
            ],
        )
        self.assertEqual(
            at_from_db[0]["actions"],
            [
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
        )
        self.assertEqual(
            at_from_db[0]["report"],
            {
                "recipientIds": ["111", "113"],
                "template": "hey {{username}}, this is a report!",
                "options": {},
                "enabled": True,
            },
        )
        self.assertEqual(at_from_db[0]["enabled"], True)
        self.assertEqual(
            at_from_db[0]["createdAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        )
        self.assertEqual(
            at_from_db[0]["updatedAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        )
        self.assertEqual(at_from_db[0]["id"], "hafujwe09023")

    def test_save_to_db_dict_instance(self):
        instance = MongoSingleton.get_instance()
        client = instance.get_client()

        load_dotenv()
        db_name = os.getenv("AUTOMATION_DB_NAME")
        collection_name = os.getenv("AUTOMATION_DB_COLLECTION")
        today_created_at = datetime.now(tz=timezone.utc)

        client[db_name].drop_collection(collection_name)

        automation_dict = {
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
        }

        automation_db = AutomationDB()

        automation_db.save_to_db(automation_dict)

        cursor = client[db_name][collection_name].find({"guildId": "123"}, {"_id": 0})

        at_from_db = list(cursor)

        self.assertEqual(len(at_from_db), 1)
        self.assertEqual(at_from_db[0]["guildId"], "123")
        self.assertEqual(
            at_from_db[0]["triggers"],
            [
                {"options": {"category": "all_new_active"}, "enabled": True},
            ],
        )
        self.assertEqual(
            at_from_db[0]["actions"],
            [
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
        )
        self.assertEqual(
            at_from_db[0]["report"],
            {
                "recipientIds": ["111", "113"],
                "template": "hey {{username}}, this is a report!",
                "options": {},
                "enabled": True,
            },
        )
        self.assertEqual(at_from_db[0]["enabled"], True)
        self.assertEqual(
            at_from_db[0]["createdAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        )
        self.assertEqual(
            at_from_db[0]["updatedAt"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            today_created_at.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        )

    def test_raise_error(self):
        """
        give the save_to_db a type that it raises an error
        """
        automation_db = AutomationDB()

        try:
            automation_db.save_to_db(automation="this was an automation")
        except TypeError as exp:
            msg = "Not supported the type of entered object!,"
            msg += f"given type is: {type('ss')}"
            self.assertEqual(str(exp), msg)
