from datetime import datetime, timedelta

import numpy as np
from discord_analyzer.analysis.utils.member_activity_utils import get_users_past_window

from .utils.analyzer_setup import launch_db_access


def test_get_past_7_days_heatmap_users_available_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[guildId].drop_collection("heatmaps")

    db_access.db_mongo_client[guildId].create_collection("heatmaps")

    heatmaps_data = []
    acc_names = []
    for i in range(250):
        date = start_date + timedelta(days=i)
        account = f"9739932992810762{i}"
        document = {
            "date": date.strftime("%Y-%m-%d"),
            "channelId": "1020707129214111827",
            "thr_messages": list(np.zeros(24)),
            "lone_messages": list(np.zeros(24)),
            "replier": list(np.zeros(24)),
            "replied": list(np.zeros(24)),
            "mentioner": list(np.zeros(24)),
            "mentioned": list(np.zeros(24)),
            "reacter": list(np.zeros(24)),
            "reacted": list(np.zeros(24)),
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
            "account_name": account,
        }

        heatmaps_data.append(document)
        acc_names.append(account)

    db_access.db_mongo_client[guildId]["heatmaps"].insert_many(heatmaps_data)

    start_date = datetime(2023, 1, 1) + timedelta(days=243)

    user_names = get_users_past_window(
        start_date.strftime("%Y-%m-%d"), db_access.db_mongo_client[guildId]["heatmaps"]
    )

    print(set(user_names))
    print(set(acc_names[-6:]))

    assert set(user_names) == set(acc_names[-7:])


def test_get_past_7_days_heatmap_users_no_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[guildId].drop_collection("heatmaps")

    db_access.db_mongo_client[guildId].create_collection("heatmaps")

    start_date = datetime(2023, 1, 1) + timedelta(days=243)

    user_names = get_users_past_window(
        start_date.strftime("%Y-%m-%d"), db_access.db_mongo_client[guildId]["heatmaps"]
    )

    assert user_names == []
