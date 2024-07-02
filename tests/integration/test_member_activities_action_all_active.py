from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


class TestMemberActivitiesActionsAllActive(TestCase):
    def setUp(self) -> None:
        self.platform_id = "60d5ec44f9a3c2b6d7e2d11a"
        self.db_access = launch_db_access(self.platform_id)

    def test_single_user_action(self):
        self.db_access.db_mongo_client[self.platform_id].drop_collection("heatmaps")
        users_id_list = ["user1"]
        analyzer = analyzer = setup_platform(
            self.db_access,
            self.platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
        )

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
                    "channel_id": "1020707129214111827",
                    "thread_id": None,
                },
                "source_id": f"11188143219343360{i}",
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.platform_id][
            "rawmemberactivities"
        ].insert_many(rawinfo_samples)

        analyzer.recompute()
        cursor = self.db_access.db_mongo_client[self.platform_id][
            "memberactivities"
        ].find({}, {"_id": 0, "all_active": 1})

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            self.assertEqual(set(document["all_active"]), set(["user1"]))

    def test_lone_msg_action(self):
        users_id_list = ["user1", "user2", "user3"]

        analyzer = setup_platform(
            self.db_access,
            self.platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
        )
        self.db_access.db_mongo_client[self.platform_id].drop_collection("heatmaps")

        rawinfo_samples = []
        active_users = ["user1", "user2"]
        for i in range(35 * 24):
            author = active_users[i % len(active_users)]
            sample = {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "1020707129214111827",
                    "thread_id": None,
                },
                "source_id": f"11188143219343360{i}",
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.platform_id][
            "rawmemberactivities"
        ].insert_many(rawinfo_samples)

        analyzer.recompute()
        cursor = self.db_access.db_mongo_client[self.platform_id][
            "memberactivities"
        ].find({}, {"_id": 0, "all_active": 1})

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            self.assertEqual(set(document["all_active"]), set(["user1", "user2"]))

    def test_thr_message_action(self):
        users_id_list = ["user1", "user2", "user3", "user4"]
        analyzer = setup_platform(
            self.db_access,
            self.platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
        )
        self.db_access.db_mongo_client[self.platform_id].drop_collection("heatmaps")

        rawinfo_samples = []
        active_users = ["user1", "user2"]
        for i in range(35 * 24):
            author = active_users[i % len(active_users)]
            sample = {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "1020707129214111827",
                    "thread_id": f"19191{i % 5}",
                },
                "source_id": f"11188143219343360{i}",
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.platform_id][
            "rawmemberactivities"
        ].insert_many(rawinfo_samples)

        analyzer.recompute()
        cursor = self.db_access.db_mongo_client[self.platform_id][
            "memberactivities"
        ].find({}, {"_id": 0, "all_active": 1, "date": 1})

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            print(document)
            self.assertEqual(set(document["all_active"]), set(["user1", "user2"]))
