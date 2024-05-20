# test analyzing memberactivities
from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_analyzer_week_period_recompute_empty_analytics():
    """
    test the whole analyzer pipeline for a guild with a week period
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
        db_access, platform_id, guildId, discordId_list=acc_id, days_ago_period=7
    )

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

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

    analyzer = setup_analyzer(guildId, platform_id)
    analyzer.recompute_analytics()

    memberactivities_cursor = db_access.db_mongo_client[guildId][
        "memberactivities"
    ].find({})
    memberactivities_data = list(memberactivities_cursor)

    date = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # just one document in memberactivities
    assert len(memberactivities_data) == 1
    assert memberactivities_data[0]["date"] == date.isoformat()

    heatmaps_cursor = db_access.db_mongo_client[guildId]["heatmaps"].find({})
    heatmaps_data = list(heatmaps_cursor)

    # 7 days, multiplied with 2
    # (accounts are: "973993299281076285", "remainder")
    assert len(heatmaps_data) == 14
    # last document must be for yesterday
    assert heatmaps_data[-1]["date"] == date.strftime("%Y-%m-%d")
