# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_all_joined_day_members():
    """
    testing the all_joined_day
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)
    today = datetime.now()

    acc_id = [
        "user_0",
        "user_1",
    ]
    # users joining 15 days ago
    # and 13 days ago
    acc_join_dates = [
        today - timedelta(days=15),
        today - timedelta(days=13),
    ]

    analyzer = setup_platform(
        db_access,
        platform_id,
        discordId_list=acc_id,
        dates=acc_join_dates,
        days_ago_period=30,
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    rawinfo_samples = []

    # generating random rawinfo data
    for i in range(150):
        author = np.random.choice(acc_id)
        replied_user = np.random.choice(acc_id)
        samples = [
            {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": [replied_user],
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
                "author_id": replied_user,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [
                    {"name": "reply", "type": "receiver", "users_engaged_id": [author]}
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

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
        rawinfo_samples
    )

    analyzer.run_once()

    cursor = db_access.db_mongo_client[platform_id]["memberactivities"].find([])

    memberactivities = list(cursor)

    for document in memberactivities:
        date = document["date"]

        # checking the types
        assert isinstance(document["all_joined_day"], list)
        assert isinstance(document["all_joined"], list)

        joined_day = set(document["all_joined_day"])
        joined = set(document["all_joined"])

        if (today - date).days == 15:
            assert joined_day == {"user_0"}
            assert joined == {"user_0"}
        elif (today - date).days == 14:
            assert joined_day == set()
            assert joined == {"user_0"}
        elif (today - date).days == 13:
            assert joined_day == {"user_1"}
            assert joined == {"user_0", "user_1"}
        elif (today - date).days == 12:
            assert joined_day == set()
            assert joined == {"user_1", "user_0"}
        elif (today - date).days == 11:
            assert joined_day == set()
            assert joined == {"user_1", "user_0"}
        elif (today - date).days == 10:
            assert joined_day == set()
            assert joined == {"user_1", "user_0"}
        elif (today - date).days == 9:
            assert joined_day == set()
            assert joined == {"user_1", "user_0"}
        elif (today - date).days == 8:
            assert joined_day == set()
            assert joined == {"user_1", "user_0"}
        elif (today - date).days == 7:
            assert joined_day == set()
            assert joined == {"user_1"}
        elif (today - date).days == 6:
            assert joined_day == set()
            assert joined == {"user_1"}
        else:
            assert joined_day == set()
            assert joined == set()
