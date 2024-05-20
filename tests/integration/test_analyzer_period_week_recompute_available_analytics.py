# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.mock_heatmaps import create_empty_heatmaps_data
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.remove_and_setup_guild import setup_db_guild


def test_analyzer_week_period_recompute_available_analytics():
    """
    We're assuming our period was 7 days and
    analytics was done for 1 day and we're continuing the analytics today
    and use run_once method with empty analytics available
    """
    # first create the collections
    guildId = "1234"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(guildId)

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]
    setup_db_guild(
        db_access, platform_id, guildId, discordId_list=acc_id, days_ago_period=8
    )

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # filling memberactivities with some data
    memberactivity_data = create_empty_memberactivities_data(
        datetime.now() - timedelta(days=2), count=1
    )
    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        memberactivity_data
    )

    # filling heatmaps with some data
    heatmaps_data = create_empty_heatmaps_data(
        datetime.now() - timedelta(days=7), count=1
    )
    db_access.db_mongo_client[guildId]["heatmaps"].insert_many(heatmaps_data)

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # 24 hour * 7 days
    for i in range(168):
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

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer(guildId)
    analyzer.recompute_analytics()

    memberactivities_cursor = db_access.query_db_find("memberactivities", {})
    memberactivities_data = list(memberactivities_cursor)
    yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    print("memberactivities_data: ", memberactivities_data)

    memberactivities_expected_dates = [
        yesterday.isoformat(),
        (yesterday - timedelta(days=1)).isoformat(),
    ]

    # two documents in memberactivities
    assert len(memberactivities_data) == 2
    for document in memberactivities_data:
        assert document["date"] in memberactivities_expected_dates

    heatmaps_cursor = db_access.query_db_find(
        "heatmaps", {}, feature_projection=None, sorting=("yesterday", -1)
    )
    heatmaps_data = list(heatmaps_cursor)

    print("heatmaps_data: ", heatmaps_data)

    heatmaps_expected_dates = [
        yesterday.strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=1)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=2)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=3)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=4)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=5)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=6)).strftime("%Y-%m-%d"),
        (yesterday - timedelta(days=7)).strftime("%Y-%m-%d"),
    ]
    # 7 days, multiplied with 2
    # (accounts are: "973993299281076285", "973993299281076286")
    assert len(heatmaps_data) == 14
    # last document must be for yesterday

    for document in heatmaps_data:
        document["date"] in heatmaps_expected_dates
