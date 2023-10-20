from datetime import datetime, timedelta

import numpy as np

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_excluding_bots_heatmaps():
    """
    test if we're excluding bots from analyzer pipeline
    """
    guildId = "1234567"
    db_access = launch_db_access(guildId)

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
    setup_db_guild(
        db_access,
        guildId,
        discordId_list=acc_id,
        discordId_isbot=acc_isbots,
        days_ago_period=connected_days_before,
    )
    window_start_date = datetime.now() - timedelta(days=connected_days_before)

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # 24 hours
    # 30 days
    # 24 * 30
    for i in range(720):
        sample = {
            "type": 19,
            "author": acc_id[i % len(acc_id)],
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

    db_access.db_mongo_client[guildId]

    pipeline = [
        # Filter documents based on date
        {"$match": {"date": {"$gte": window_start_date.strftime("%Y-%m-%d")}}},
        {"$group": {"_id": "$account_name"}},
        {
            "$group": {
                "_id": None,
                "uniqueAccounts": {"$push": "$_id"},
            }
        },
    ]
    result = list(db_access.db_mongo_client[guildId]["heatmaps"].aggregate(pipeline))

    print(result[0]["uniqueAccounts"])
    print(f"np.array(acc_id)[acc_isbots]: {np.array(acc_id)[acc_isbots]}")

    # checking if the bots are not included in heatmaps
    for account_name in result[0]["uniqueAccounts"]:
        assert account_name not in np.array(acc_id)[acc_isbots]
