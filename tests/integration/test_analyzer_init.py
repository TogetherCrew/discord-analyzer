from datetime import datetime, timedelta

from analyzer_init import AnalyzerInit
from bson.objectid import ObjectId
from utils.mongo import MongoSingleton


def test_analyzer_init():
    platform_id = "515151515151515151515151"
    days_ago_period = 30
    community_id = "aabbccddeeff001122334455"
    guildId = "1234"

    mongo_client = MongoSingleton.get_instance().get_client()
    mongo_client["Core"]["platforms"].delete_one({"metadata.id": guildId})
    mongo_client.drop_database(guildId)

    act_param = {
        "INT_THR": 1,
        "UW_DEG_THR": 1,
        "PAUSED_T_THR": 1,
        "CON_T_THR": 4,
        "CON_O_THR": 3,
        "EDGE_STR_THR": 5,
        "UW_THR_DEG_THR": 5,
        "VITAL_T_THR": 4,
        "VITAL_O_THR": 3,
        "STILL_T_THR": 2,
        "STILL_O_THR": 2,
        "DROP_H_THR": 2,
        "DROP_I_THR": 1,
    }
    window = {
        "period_size": 7,
        "step_size": 1,
    }

    mongo_client["Core"]["platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guildId,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "selectedChannels": ["1020707129214111827"],
                "window": window,
                "action": act_param,
                "period": datetime.now() - timedelta(days=days_ago_period),
            },
            "community": ObjectId(community_id),
            "disconnectedAt": None,
            "connectedAt": (datetime.now() - timedelta(days=days_ago_period + 10)),
            "isInProgress": True,
            "createdAt": datetime(2023, 11, 1),
            "updatedAt": datetime(2023, 11, 1),
        }
    )

    analyzer = AnalyzerInit(guildId)

    mongo_client[guildId]["guildmembers"].insert_one(
        {
            "discordId": "user1",
            "username": "sample_user1",
            "roles": ["1012430565959553145"],
            "joinedAt": datetime.now() - timedelta(days=5),
            "avatar": "3ddd6e429f75d6a711d0a58ba3060694",
            "isBot": False,
            "discriminator": "0",
        }
    )
    mongo_client[guildId].create_collection("heatmaps")
    mongo_client[guildId].create_collection("memberactivities")

    # generating random rawinfo data
    # 24 hours
    # 90 days
    rawinfo_samples = []
    for i in range(24 * days_ago_period):
        sample = {
            "type": 19,
            "author": "user1",
            "content": f"test{i}",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "user2",
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"11188143219343360{i}",
            "channelId": "1020707129214111827",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        rawinfo_samples.append(sample)

    mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    tc_discord_analyzer = analyzer.get_analyzer()

    tc_discord_analyzer.recompute_analytics()

    heatmaps_data = mongo_client[guildId]["heatmaps"].find_one({})
    assert heatmaps_data is not None

    memberactivities_data = mongo_client[guildId]["memberactivities"].find_one({})
    assert memberactivities_data is not None
