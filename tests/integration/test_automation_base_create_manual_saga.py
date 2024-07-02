import unittest

from tc_analyzer_lib.automation.utils.automation_base import AutomationBase

from .utils.analyzer_setup import launch_db_access


class TestManualSagaCreation(unittest.TestCase):
    def test_create_manual_saga(self):
        guild_id = "1234"

        db_access = launch_db_access(guild_id)
        db_access.db_mongo_client["Saga"].drop_collection("sagas")
        automation_base = AutomationBase()

        data = {
            "guildId": guild_id,
            "created": False,
            "discordId": "user1",
            "message": "This message is sent you for notifications!",
            "userFallback": True,
        }

        saga_id = automation_base._create_manual_saga(data)

        manual_saga = db_access.db_mongo_client["Saga"]["sagas"].find_one(
            {"sagaId": saga_id}
        )

        self.assertEqual(manual_saga["choreography"]["name"], "DISCORD_NOTIFY_USERS")
        self.assertEqual(
            manual_saga["choreography"]["transactions"],
            [
                {
                    "queue": "DISCORD_BOT",
                    "event": "SEND_MESSAGE",
                    "order": 1,
                    "status": "NOT_STARTED",
                }
            ],
        )
        self.assertEqual(manual_saga["status"], "IN_PROGRESS")
        self.assertEqual(manual_saga["data"], data)
        self.assertEqual(manual_saga["sagaId"], saga_id)
