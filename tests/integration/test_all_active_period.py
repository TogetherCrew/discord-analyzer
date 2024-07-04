from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_two_weeks_period_active_members():
    """
    test all_active members for the two weeks period in the new schema
    """
    platform_id = "60d5ec44f9a3c2b6d7e2d11a"
    db_access = launch_db_access(platform_id)

    acc_id = [
        "user0",
        "user1",
        "user2",
        "user3",
    ]

    # A guild connected at 35 days ago
    connected_days_before = 35
    analyzer = setup_platform(
        db_access,
        platform_id,
        discordId_list=acc_id,
        days_ago_period=connected_days_before,
        resources=["1020707129214111827", "general_id"],
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")
    db_access.db_mongo_client[platform_id].drop_collection("rawmemberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # A message from user0 to user1 on day 0 of past two weeks
    sample = {
        "actions": [{"name": "message", "type": "emitter"}],
        "author_id": acc_id[0],
        "date": datetime.now() - timedelta(days=14),
        "interactions": [
            {"name": "reply", "type": "emitter", "users_engaged_id": [acc_id[1]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "general_id",
            "thread_id": None,
        },
        "source_id": "111881432193433601",
    }
    sample2 = {
        "actions": [],
        "author_id": acc_id[1],
        "date": datetime.now() - timedelta(days=14),
        "interactions": [
            {"name": "reply", "type": "receiver", "users_engaged_id": [acc_id[0]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "general_id",
            "thread_id": None,
        },
        "source_id": "111881432193433601",
    }

    rawinfo_samples.append(sample)
    rawinfo_samples.append(sample2)

    # A message from user1 to user0 on day 0 of past two weeks
    sample = {
        "actions": [{"name": "message", "type": "emitter"}],
        "author_id": acc_id[1],
        "date": datetime.now() - timedelta(days=14),
        "interactions": [
            {"name": "reply", "type": "emitter", "users_engaged_id": [acc_id[0]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "1020707129214111827",
            "thread_id": None,
        },
        "source_id": "111881432193433602",
    }
    sample2 = {
        "actions": [],
        "author_id": acc_id[0],
        "date": datetime.now() - timedelta(days=14),
        "interactions": [
            {"name": "reply", "type": "receiver", "users_engaged_id": [acc_id[1]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "general_id",
            "thread_id": None,
        },
        "source_id": "111881432193433602",
    }

    rawinfo_samples.append(sample)
    rawinfo_samples.append(sample2)

    # A message from user2 to user3 on day 3 of past two weeks
    sample = {
        "actions": [{"name": "message", "type": "emitter"}],
        "author_id": acc_id[2],
        "date": datetime.now() - timedelta(days=(14 - 3)),
        "interactions": [
            {"name": "reply", "type": "emitter", "users_engaged_id": [acc_id[3]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "1020707129214111827",
            "thread_id": None,
        },
        "source_id": "111881432193433603",
    }
    sample2 = {
        "actions": [],
        "author_id": acc_id[3],
        "date": datetime.now() - timedelta(days=(14 - 3)),
        "interactions": [
            {"name": "reply", "type": "receiver", "users_engaged_id": [acc_id[2]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "1020707129214111827",
            "thread_id": None,
        },
        "source_id": "111881432193433603",
    }

    rawinfo_samples.append(sample)
    rawinfo_samples.append(sample2)

    # A message from user3 to user2 on day 3 of past two weeks
    sample = {
        "actions": [{"name": "message", "type": "emitter"}],
        "author_id": acc_id[3],
        "date": datetime.now() - timedelta(days=(14 - 3)),
        "interactions": [
            {"name": "reply", "type": "emitter", "users_engaged_id": [acc_id[2]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "1020707129214111827",
            "thread_id": None,
        },
        "source_id": "111881432193433604",
    }
    sample2 = {
        "actions": [],
        "author_id": acc_id[2],
        "date": datetime.now() - timedelta(days=(14 - 3)),
        "interactions": [
            {"name": "reply", "type": "receiver", "users_engaged_id": [acc_id[3]]}
        ],
        "metadata": {
            "bot_activity": False,
            "channel_id": "1020707129214111827",
            "thread_id": None,
        },
        "source_id": "111881432193433604",
    }
    rawinfo_samples.append(sample)
    rawinfo_samples.append(sample2)

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
        rawinfo_samples
    )

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
        date = activities["date"].date()
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
