from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_two_weeks_period_active_members():
    """
    test all_active members for the two weeks period in the new schema
    """
    guildId = "1234567"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(guildId)

    acc_id = [
        "user0",
        "user1",
        "user2",
        "user3",
    ]

    # A guild connected at 35 days ago
    connected_days_before = 35
    setup_db_guild(
        db_access,
        platform_id,
        guildId,
        discordId_list=acc_id,
        days_ago_period=connected_days_before,
    )

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # A message from user0 to user1 on day 0 of past two weeks
    sample = {
        "type": 19,
        "author": acc_id[0],
        "content": "test_message_0",
        "user_mentions": [],
        "role_mentions": [],
        "reactions": [],
        "replied_user": acc_id[1],
        "createdDate": (datetime.now() - timedelta(days=14)),
        "messageId": "111881432193433601",
        "channelId": "1020707129214111827",
        "channelName": "general",
        "threadId": None,
        "threadName": None,
        "isGeneratedByWebhook": False,
    }

    rawinfo_samples.append(sample)

    # A message from user1 to user0 on day 0 of past two weeks
    sample = {
        "type": 19,
        "author": acc_id[1],
        "content": "test_message_1",
        "user_mentions": [],
        "role_mentions": [],
        "reactions": [],
        "replied_user": acc_id[0],
        "createdDate": (datetime.now() - timedelta(days=14)),
        "messageId": "111881432193433602",
        "channelId": "1020707129214111827",
        "channelName": "general",
        "threadId": None,
        "threadName": None,
        "isGeneratedByWebhook": False,
    }

    rawinfo_samples.append(sample)

    # A message from user2 to user3 on day 3 of past two weeks
    sample = {
        "type": 19,
        "author": acc_id[2],
        "content": "test_message_1",
        "user_mentions": [],
        "role_mentions": [],
        "reactions": [],
        "replied_user": acc_id[3],
        "createdDate": (datetime.now() - timedelta(days=(14 - 3))),
        "messageId": "111881432193433603",
        "channelId": "1020707129214111827",
        "channelName": "general",
        "threadId": None,
        "threadName": None,
        "isGeneratedByWebhook": False,
    }

    rawinfo_samples.append(sample)

    # A message from user3 to user2 on day 3 of past two weeks
    sample = {
        "type": 19,
        "author": acc_id[3],
        "content": "test_message_1",
        "user_mentions": [],
        "role_mentions": [],
        "reactions": [],
        "replied_user": acc_id[2],
        "createdDate": (datetime.now() - timedelta(days=(14 - 3))),
        "messageId": "111881432193433604",
        "channelId": "1020707129214111827",
        "channelName": "general",
        "threadId": None,
        "threadName": None,
        "isGeneratedByWebhook": False,
    }

    rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer(guildId, platform_id)
    analyzer.run_once()

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities",
        {},
        feature_projection={"_id": 0, "date": 1, "all_active": 1},
    )
    memberactivities = list(memberactivities_cursor)

    # print(f"memberactivities: {memberactivities}")

    date_now = datetime.now()

    for activities in memberactivities:
        date = datetime.fromisoformat(activities["date"]).date()
        # print("date: ", date)
        # 14 days minues 7
        if date == (date_now - timedelta(days=14)).date():
            # print("time delta days: 14")
            assert set(activities["all_active"]) == set(["user0", "user1"])
        elif date == (date_now - timedelta(days=13)).date():
            # print("time delta days: 13")
            assert set(activities["all_active"]) == set(["user0", "user1"])
        elif date == (date_now - timedelta(days=12)).date():
            # print("time delta days: 12")
            assert set(activities["all_active"]) == set(["user0", "user1"])
        elif date == (date_now - timedelta(days=11)).date():
            # print("time delta days: 11")
            assert set(activities["all_active"]) == set(
                ["user0", "user1", "user2", "user3"]
            )
        elif date == (date_now - timedelta(days=10)).date():
            # print("time delta days: 10")
            assert set(activities["all_active"]) == set(
                ["user0", "user1", "user2", "user3"]
            )
        elif date == (date_now - timedelta(days=9)).date():
            # print("time delta days: 9")
            assert set(activities["all_active"]) == set(
                ["user0", "user1", "user2", "user3"]
            )
        elif date == (date_now - timedelta(days=8)).date():
            # print("time delta days: 8")
            assert set(activities["all_active"]) == set(
                ["user0", "user1", "user2", "user3"]
            )
        elif date == (date_now - timedelta(days=7)).date():
            # print("time delta days: 7")
            assert set(activities["all_active"]) == set(["user2", "user3"])
        elif date == (date_now - timedelta(days=6)).date():
            # print("time delta days: 6")
            assert set(activities["all_active"]) == set(["user2", "user3"])
        elif date == (date_now - timedelta(days=5)).date():
            # print("time delta days: 5")
            assert set(activities["all_active"]) == set(["user2", "user3"])
        else:
            # print("time delta days: else")
            assert set(activities["all_active"]) == set()
