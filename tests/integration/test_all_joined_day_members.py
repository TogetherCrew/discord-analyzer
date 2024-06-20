# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_all_joined_day_members():
    """
    testing the all_joined_day
    """
    guildId = "1234"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(guildId)
    today = datetime.now()

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]
    # users joining 15 days ago
    # and 13 days ago
    acc_join_dates = [
        today - timedelta(days=15),
        today - timedelta(days=13),
    ]

    setup_db_guild(
        db_access,
        platform_id,
        discordId_list=acc_id,
        dates=acc_join_dates,
        days_ago_period=30,
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")
    db_access.db_mongo_client[platform_id].create_collection("heatmaps")
    db_access.db_mongo_client[platform_id].create_collection("memberactivities")

    rawinfo_samples = []

    # generating random rawinfo data
    for i in range(150):
        sample = {
            "type": 19,
            "author": np.random.choice(acc_id),
            "content": f"test{i}",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": np.random.choice(acc_id),
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"11188143219343360{i}",
            "channelId": "1020707129214111827",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer(platform_id)
    analyzer.run_once()

    cursor = db_access.db_mongo_client[platform_id]["memberactivities"].find([])

    memberactivities = list(cursor)

    for document in memberactivities:
        date_str = document["date"].split("T")[0]
        date = datetime.strptime(date_str, "%Y-%m-%d")

        # checking the types
        assert isinstance(document["all_joined_day"], list)
        assert isinstance(document["all_joined"], list)

        joined_day = set(document["all_joined_day"])
        joined = set(document["all_joined"])

        if (today - date).days == 15:
            assert joined_day == {"973993299281076285"}
            assert joined == {"973993299281076285"}
        elif (today - date).days == 14:
            assert joined_day == set()
            assert joined == {"973993299281076285"}
        elif (today - date).days == 13:
            assert joined_day == {"973993299281076286"}
            assert joined == {"973993299281076285", "973993299281076286"}
        elif (today - date).days == 12:
            assert joined_day == set()
            assert joined == {"973993299281076286", "973993299281076285"}
        elif (today - date).days == 11:
            assert joined_day == set()
            assert joined == {"973993299281076286", "973993299281076285"}
        elif (today - date).days == 10:
            assert joined_day == set()
            assert joined == {"973993299281076286", "973993299281076285"}
        elif (today - date).days == 9:
            assert joined_day == set()
            assert joined == {"973993299281076286", "973993299281076285"}
        elif (today - date).days == 8:
            assert joined_day == set()
            assert joined == {"973993299281076286", "973993299281076285"}
        elif (today - date).days == 7:
            assert joined_day == set()
            assert joined == {"973993299281076286"}
        elif (today - date).days == 6:
            assert joined_day == set()
            assert joined == {"973993299281076286"}
        else:
            assert joined_day == set()
            assert joined == set()
