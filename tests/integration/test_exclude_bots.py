from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_excluding_bots_heatmaps():
    """
    test if we're excluding bots from analyzer pipeline
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    acc_id = [
        "user0",
        "user1",
        "user2",
        "user3",
        "bot0",
        "bot1",
        "bot2",
    ]
    acc_isbots = [False, False, False, False, True, True, True]

    # A guild connected at 35 days ago
    connected_days_before = 35
    analyzer = setup_platform(
        db_access,
        platform_id,
        discordId_list=acc_id,
        discordId_isbot=acc_isbots,
        days_ago_period=connected_days_before,
    )
    window_start_date = datetime.now() - timedelta(days=connected_days_before)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # 24 hours
    # 30 days
    # 24 * 30
    for i in range(720):
        author = acc_id[i % len(acc_id)]
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

    db_access.db_mongo_client[platform_id]

    pipeline = [
        # Filter documents based on date
        {"$match": {"date": {"$gte": window_start_date}}},
        {"$group": {"_id": "$user"}},
        {
            "$group": {
                "_id": None,
                "uniqueAccounts": {"$push": "$_id"},
            }
        },
    ]
    result = list(
        db_access.db_mongo_client[platform_id]["heatmaps"].aggregate(pipeline)
    )

    # print(result[0]["uniqueAccounts"])
    # print(f"np.array(acc_id)[acc_isbots]: {np.array(acc_id)[acc_isbots]}")

    # checking if the bots are not included in heatmaps
    for account_name in result[0]["uniqueAccounts"]:
        assert account_name not in np.array(acc_id)[acc_isbots]
