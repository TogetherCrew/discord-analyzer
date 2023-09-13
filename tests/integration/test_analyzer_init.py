from datetime import datetime, timedelta

from analyzer_init import AnalyzerInit
from pymongo import MongoClient
from utils.daolytics_uitls import get_mongo_credentials


def test_analyzer_init():
    analyzer = AnalyzerInit()

    guildId = "1234"
    days_ago_period = 30
    mongo_creds = get_mongo_credentials()
    user = mongo_creds["user"]
    password = mongo_creds["password"]
    host = mongo_creds["host"]
    port = mongo_creds["port"]

    url = f"mongodb://{user}:{password}@{host}:{port}"

    mongo_client: MongoClient = MongoClient(url)

    mongo_client["RnDAO"]["guilds"].delete_one({"guildId": guildId})
    mongo_client.drop_database(guildId)

    mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guildId,
            "user": "876487027099582524",
            "name": "Sample Guild",
            "connectedAt": (datetime.now() - timedelta(days=10)),
            "isInProgress": True,
            "isDisconnected": False,
            "icon": "afd0d06fd12b2905c53708ca742e6c66",
            "window": [7, 1],
            "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 3, 2, 2, 1],
            "selectedChannels": [
                {
                    "channelId": "1020707129214111827",
                    "channelName": "general",
                },
            ],
            "period": (datetime.now() - timedelta(days=days_ago_period)),
        }
    )

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
        }
        rawinfo_samples.append(sample)

    mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    tc_discord_analyzer, mongo_creds = analyzer.get_analyzer()

    tc_discord_analyzer.recompute_analytics(guildId)

    heatmaps_data = mongo_client[guildId]["heatmaps"].find_one({})
    assert heatmaps_data is not None

    memberactivities_data = mongo_client[guildId]["memberactivities"].find_one({})
    assert memberactivities_data is not None
