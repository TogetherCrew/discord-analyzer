from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


class TestMemberActivitiesActionsAllActivities(TestCase):
    def setUp(self) -> None:
        self.platformId = "1234"
        self.db_access = launch_db_access(self.platformId)

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

        analyzer = setup_platform(
            self.db_access,
            platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
            action=action,
            resources=["123"],
        )
        self.db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

        rawinfo_samples = []
        for i in range(35 * 24):
            author = "user1"
            sample = {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "123",
                    "thread_id": None,
                },
                "source_id": f"11188143219343360{i}",
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
            rawinfo_samples
        )

        analyzer.recompute()
        cursor = self.db_access.db_mongo_client[platform_id]["memberactivities"].find(
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
