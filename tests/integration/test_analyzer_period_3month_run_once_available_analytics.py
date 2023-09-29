# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.mock_heatmaps import create_empty_heatmaps_data
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.remove_and_setup_guild import setup_db_guild


def test_analyzer_three_month_period_run_once_available_analytics():
    """
    test the whole analyzer pipeline for a guild with a week period
    and use run_once method with empty analytics available
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]

    setup_db_guild(db_access, guildId, discordId_list=acc_id, days_ago_period=90)

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # filling memberactivities with some data
    # filling heatmaps with some data
    # filling up to 2 days ago with 83 documents
    memberactivity_data = create_empty_memberactivities_data(
        datetime.now() - timedelta(days=84), count=83
    )
    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        memberactivity_data
    )

    # filling heatmaps with some data
    # filling up to 2 days ago with 89 documents
    # just yesterday is left to be analyzed
    heatmaps_data = create_empty_heatmaps_data(
        datetime.now() - timedelta(days=90), count=89
    )
    db_access.db_mongo_client[guildId]["heatmaps"].insert_many(heatmaps_data)

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # 24 hours, 90 days
    # 24 * 90 = 720
    for i in range(24 * 90):
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

    analyzer = setup_analyzer()
    analyzer.run_once(guildId=guildId)

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities", {}, sorting=("date", -1)
    )
    memberactivities_data = list(memberactivities_cursor)

    yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    assert len(memberactivities_data) == 84
    assert memberactivities_data[0]["date"] == yesterday.isoformat()
    # yesterday is `-1` day and so
    # we would use 83 days ago rather than 84
    document_start_date = yesterday - timedelta(days=83)
    assert memberactivities_data[-1]["date"] == (document_start_date).isoformat()

    heatmaps_cursor = db_access.query_db_find("heatmaps", {}, sorting=("date", -1))
    heatmaps_data = list(heatmaps_cursor)

    # 89 days of 1 document plus the last day as 2 documents
    # as we have 2 accounts
    # (accounts are: "973993299281076285", "973993299281076286")
    assert len(heatmaps_data) == 89 + 2
    # checking first and last document
    assert heatmaps_data[0]["date"] == yesterday.strftime("%Y-%m-%d")
    month_ago = yesterday - timedelta(89)
    assert heatmaps_data[-1]["date"] == month_ago.strftime("%Y-%m-%d")
