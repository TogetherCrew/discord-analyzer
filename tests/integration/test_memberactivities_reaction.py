from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


class TestMemberActivitiesReactions(TestCase):
    def setUp(self) -> None:
        self.platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.db_access = launch_db_access(self.platform_id)

    def test_single_user_action(self):
        """
        just actions and no interaction
        """
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

        setup_db_guild(
            self.db_access,
            self.platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
            action=action,
        )
        self.db_access.db_mongo_client[self.platform_id].drop_collection("heatmaps")
        self.db_access.db_mongo_client[self.platform_id].drop_collection(
            "rawmemberactivities"
        )

        rawinfo_samples = []
        for i in range(35 * 24):
            author = "user1"
            reacted_user = "user2"
            samples = [
                {
                    "actions": [{"name": "message", "type": "emitter"}],
                    "author_id": author,
                    "date": datetime.now() - timedelta(hours=i),
                    "interactions": [
                        {
                            "name": "reaction",
                            "type": "receiver",
                            "users_engaged_id": [reacted_user],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "1020707129214111827",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
                {
                    "actions": [],
                    "author_id": reacted_user,
                    "date": datetime.now() - timedelta(hours=i),
                    "interactions": [
                        {
                            "name": "reaction",
                            "type": "emitter",
                            "users_engaged_id": [author],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "1020707129214111827",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
            ]
            rawinfo_samples.extend(samples)

        self.db_access.db_mongo_client[self.platform_id][
            "rawmemberactivities"
        ].insert_many(rawinfo_samples)
        analyzer = setup_analyzer(self.platform_id)
        analyzer.recompute_analytics()
        cursor = self.db_access.db_mongo_client[self.platform_id][
            "memberactivities"
        ].find(
            {},
            {
                "_id": 0,
                "all_active": 1,
                "date": 1,
            },
        )

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            # user1 was sending channel messages (lone_message)
            # user2 was doing reactions
            self.assertEqual(set(document["all_active"]), set(["user1", "user2"]))
