from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


class TestMemberActivitiesActionsAllActive(TestCase):
    def setUp(self) -> None:
        self.guildId = "1234"
        self.db_access = launch_db_access(self.guildId)

    def test_single_user_action(self):
        platform_id = "515151515151515151515151"
        users_id_list = ["user1"]
        setup_db_guild(
            self.db_access,
            platform_id,
            self.guildId,
            discordId_list=users_id_list,
            days_ago_period=35,
        )
        self.db_access.db_mongo_client[self.guildId]["heatmaps"].delete_many({})
        self.db_access.db_mongo_client[self.guildId].create_collection("heatmaps")

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

        self.db_access.db_mongo_client[self.guildId]["rawinfos"].insert_many(
            rawinfo_samples
        )
        analyzer = setup_analyzer(self.guildId, platform_id)
        analyzer.recompute_analytics(self.guildId)
        cursor = self.db_access.db_mongo_client[self.guildId]["memberactivities"].find(
            {}, {"_id": 0, "all_active": 1}
        )

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            self.assertEqual(set(document["all_active"]), set(["user1"]))

    def test_lone_msg_action(self):
        users_id_list = ["user1", "user2", "user3"]
        platform_id = "515151515151515151515151"

        setup_db_guild(
            self.db_access,
            platform_id,
            self.guildId,
            discordId_list=users_id_list,
            days_ago_period=35,
        )
        self.db_access.db_mongo_client[self.guildId]["heatmaps"].delete_many({})
        self.db_access.db_mongo_client[self.guildId].create_collection("heatmaps")

        rawinfo_samples = []
        active_users = ["user1", "user2"]
        for i in range(35 * 24):
            sample = {
                "type": 0,
                "author": active_users[i % len(active_users)],
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

        self.db_access.db_mongo_client[self.guildId]["rawinfos"].insert_many(
            rawinfo_samples
        )
        analyzer = setup_analyzer(self.guildId, platform_id)
        analyzer.recompute_analytics(self.guildId)
        cursor = self.db_access.db_mongo_client[self.guildId]["memberactivities"].find(
            {}, {"_id": 0, "all_active": 1}
        )

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            self.assertEqual(set(document["all_active"]), set(["user1", "user2"]))

    def test_thr_message_action(self):
        platform_id = "515151515151515151515151"
        users_id_list = ["user1", "user2", "user3", "user4"]
        setup_db_guild(
            self.db_access,
            platform_id,
            self.guildId,
            discordId_list=users_id_list,
            days_ago_period=35,
        )
        self.db_access.db_mongo_client[self.guildId]["heatmaps"].delete_many({})
        self.db_access.db_mongo_client[self.guildId].create_collection("heatmaps")

        rawinfo_samples = []
        active_users = ["user1", "user2"]
        for i in range(35 * 24):
            sample = {
                "type": 0,
                "author": active_users[i % len(active_users)],
                "content": f"test message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": (datetime.now() - timedelta(hours=i)),
                "messageId": f"11188143219343360{i}",
                "channelId": "1020707129214111827",
                "channelName": "general",
                "threadId": f"19191{i % 5}",
                "threadName": f"Thread_test_{i % 5}",
                "isGeneratedByWebhook": False,
            }
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.guildId]["rawinfos"].insert_many(
            rawinfo_samples
        )
        analyzer = setup_analyzer(self.guildId, platform_id)
        analyzer.recompute_analytics(self.guildId)
        cursor = self.db_access.db_mongo_client[self.guildId]["memberactivities"].find(
            {}, {"_id": 0, "all_active": 1, "date": 1}
        )

        # memberactivities
        computed_analytics = list(cursor)

        for document in computed_analytics:
            print(document)
            self.assertEqual(set(document["all_active"]), set(["user1", "user2"]))
