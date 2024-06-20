from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


class TestMemberActivitiesReply(TestCase):
    def setUp(self) -> None:
        self.guildId = "1234"
        self.db_access = launch_db_access(self.guildId)

    def test_single_user_interaction(self):
        users_id_list = ["user1", "user2"]
        action = {
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
        platform_id = "515151515151515151515151"

        setup_db_guild(
            self.db_access,
            platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
            action=action,
        )
        self.db_access.db_mongo_client[self.guildId]["heatmaps"].delete_many({})
        self.db_access.db_mongo_client[self.guildId].create_collection("heatmaps")

        rawinfo_samples = []
        for i in range(35 * 24):
            sample = {
                "type": 0,
                "author": "user1",
                "content": f"test message {i} @user2",
                "user_mentions": ["user2"],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": (datetime.now() - timedelta(hours=i)),
                "messageId": f"11188143219343360{i}",
                "channelId": "1020707129214111827",
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.guildId]["rawinfos"].insert_many(
            rawinfo_samples
        )
        analyzer = setup_analyzer(self.guildId)
        analyzer.recompute_analytics()
        cursor = self.db_access.db_mongo_client[self.guildId]["memberactivities"].find(
            {},
            {
                "_id": 0,
                "all_active": 1,
            },
        )

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            # user1 was replying user2 messages
            self.assertEqual(document["all_active"], ["user1"])
