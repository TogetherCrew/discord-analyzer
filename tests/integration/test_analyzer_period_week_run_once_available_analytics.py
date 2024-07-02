# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.mock_heatmaps import create_empty_heatmaps_data
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.setup_platform import setup_platform


def test_analyzer_week_period_run_once_available_analytics():
    """
    We're assuming our period was 7 days and
    analytics was done for 1 day and we're continuing the analytics today
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
        db_access, platform_id, discordId_list=acc_id, days_ago_period=8
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    # filling memberactivities with some data
    start_day = (datetime.now() - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    memberactivity_data = create_empty_memberactivities_data(start_day, count=1)
    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
        memberactivity_data
    )

    # filling heatmaps with some data
    start_day = (datetime.now() - timedelta(days=7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    heatmaps_data = create_empty_heatmaps_data(start_day, count=1)
    db_access.db_mongo_client[platform_id]["heatmaps"].insert_many(heatmaps_data)

    # generating rawinfo samples
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

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities", {}, sorting=("date", -1)
    )
    memberactivities_data = list(memberactivities_cursor)
    yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    print("memberactivities_data: ", memberactivities_data)

    memberactivities_expected_dates = [
        yesterday,
        (yesterday - timedelta(days=1)),
    ]

    # two documents in memberactivities
    assert len(memberactivities_data) == 2
    data = zip(memberactivities_expected_dates, memberactivities_data)
    for date, document in data:
        print("memberactivities_data Looping: ", date)
        assert document["date"] == date

    heatmaps_cursor = db_access.query_db_find("heatmaps", {}, sorting=("date", -1))
    heatmaps_data = list(heatmaps_cursor)

    print("heatmaps_data: ", heatmaps_data)

    heatmaps_expected_dates = [
        yesterday,
        yesterday,
        (yesterday - timedelta(days=1)),
        (yesterday - timedelta(days=1)),
        (yesterday - timedelta(days=2)),
        (yesterday - timedelta(days=2)),
        (yesterday - timedelta(days=3)),
        (yesterday - timedelta(days=3)),
        (yesterday - timedelta(days=4)),
        (yesterday - timedelta(days=4)),
        (yesterday - timedelta(days=5)),
        (yesterday - timedelta(days=5)),
        (yesterday - timedelta(days=6)),
        (yesterday - timedelta(days=6)),
        (yesterday - timedelta(days=7)),
    ]
    # 6 days, multiplied with 2
    # (accounts are: "973993299281076285", "973993299281076286")
    # plust 1 document we added manually
    assert len(heatmaps_data) == 13
    # last document must be for yesterday
    data = zip(heatmaps_expected_dates, heatmaps_data)
    for date, document in data:
        print("heatmaps_data Looping: ", date)
        assert document["date"] == date
