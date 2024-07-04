# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_analyzer_week_period_recompute_empty_analytics():
    """
    test the whole analyzer pipeline for a guild with a week period
    and use run_once method with empty analytics available
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]

    analyzer = setup_platform(
        db_access, platform_id, discordId_list=acc_id, days_ago_period=7
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # 24 hour * 7 days
    for i in range(168):
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

    analyzer.recompute()

    memberactivities_cursor = db_access.db_mongo_client[platform_id][
        "memberactivities"
    ].find({})
    memberactivities_data = list(memberactivities_cursor)

    date = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # just one document in memberactivities
    assert len(memberactivities_data) == 1
    assert memberactivities_data[0]["date"] == date

    heatmaps_cursor = db_access.db_mongo_client[platform_id]["heatmaps"].find({})
    heatmaps_data = list(heatmaps_cursor)

    # 7 days, multiplied with 2
    # (accounts are: "973993299281076285", "remainder")
    assert len(heatmaps_data) == 14
    # last document must be for yesterday
    assert heatmaps_data[-1]["date"] == date
