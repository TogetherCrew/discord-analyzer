from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


class TestMemberActivitiesActionsAllActivities(TestCase):
    def setUp(self) -> None:
        self.guildId = "1234"
        self.db_access = launch_db_access(self.guildId)

    def test_single_user_action(self):
        """
        just actions and no interaction
        """
        users_id_list = ["user1"]
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
        self.db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

        rawinfo_samples = []
        for i in range(35 * 24):
            sample = {
                "type": 0,
                "author": "user1",
                "content": f"test message {i}",
                "user_mentions": [],
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

        self.db_access.db_mongo_client[self.guildId]["rawmemberactivities"].insert_many(
            rawinfo_samples
        )
        analyzer = setup_analyzer(self.guildId)
        analyzer.recompute_analytics()
        cursor = self.db_access.db_mongo_client[self.guildId]["memberactivities"].find(
            {},
            {
                "_id": 0,
                "all_connected": 1,
                "all_vital": 1,
                "all_consistent": 1,
                "all_new_active": 1,
            },
        )

        # memberactivities
        computed_analytics = list(cursor)

        for idx, document in enumerate(computed_analytics):
            self.assertEqual(document["all_connected"], [])
            self.assertEqual(document["all_vital"], [])

            # first period
            if idx < 7:
                self.assertEqual(document["all_new_active"], ["user1"])
            else:
                self.assertEqual(document["all_new_active"], [])

            if idx < 14:
                self.assertEqual(document["all_consistent"], [])
            # 3rd period
            else:
                self.assertEqual(document["all_consistent"], ["user1"])
