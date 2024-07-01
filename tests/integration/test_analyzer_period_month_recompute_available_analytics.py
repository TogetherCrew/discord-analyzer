# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.mock_heatmaps import create_empty_heatmaps_data
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.setup_platform import setup_platform


def test_analyzer_month_period_recompute_available_analytics():
    """
    test the whole analyzer pipeline for a guild with 1 month period
    and use recompute method with empty analytics available
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]

    analyzer = setup_platform(db_access, platform_id, discordId_list=acc_id, days_ago_period=30)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    # filling memberactivities with some data
    # filling heatmaps with some data
    # filling up to 2 days ago with 23 documents
    memberactivity_data = create_empty_memberactivities_data(
        datetime.now() - timedelta(days=24), count=23
    )
    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
        memberactivity_data
    )

    # filling heatmaps with some data
    # filling up to 2 days ago with 29 documents
    # just yesterday is left to be analyzed
    start_day = (datetime.now() - timedelta(days=30)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    heatmaps_data = create_empty_heatmaps_data(start_day, count=29)
    db_access.db_mongo_client[platform_id]["heatmaps"].insert_many(heatmaps_data)

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # 24 * 30 = 720
    for i in range(720):
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

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities", {}, sorting=("date", -1)
    )
    memberactivities_data = list(memberactivities_cursor)

    yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    assert len(memberactivities_data) == 24
    assert memberactivities_data[0]["date"] == yesterday
    # yesterday is `-1` day and so
    # we would use 23 days ago rather than 24
    document_start_date = yesterday - timedelta(days=23)
    assert memberactivities_data[-1]["date"] == document_start_date

    heatmaps_cursor = db_access.query_db_find("heatmaps", {}, sorting=("date", -1))
    heatmaps_data = list(heatmaps_cursor)

    # 30 days, multiplied with 2
    # (accounts are: "973993299281076285", "973993299281076286")
    assert len(heatmaps_data) == 60
    # checking first and last document
    assert heatmaps_data[0]["date"] == yesterday
    month_ago = yesterday - timedelta(29)
    assert heatmaps_data[-1]["date"] == month_ago
