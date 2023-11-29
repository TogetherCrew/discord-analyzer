# test analyzing memberactivities
from datetime import datetime, timedelta

from bson.objectid import ObjectId

from .utils.analyzer_setup import launch_db_access, setup_analyzer


def test_analyzer_member_activities_from_start_empty_memberactivities():
    """
    run the analyzer for a specific guild with from_start option equal to True
    assuming the memberactivities collection is empty
    """
    # first create the collections
    guildId = "1234"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client["Core"]["Platforms"].delete_one({"metadata.id": guildId})
    db_access.db_mongo_client.drop_database(guildId)

    db_access.db_mongo_client["Core"]["Platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guildId,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "selectedChannels": [
                    {"channelId": "1020707129214111827", "channelName": "general"}
                ],
                "window": [7, 1],
                "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 3, 2, 2, 1],
                "period": datetime.now() - timedelta(days=30),
            },
            "community": ObjectId("aabbccddeeff001122334455"),
            "disconnectedAt": None,
            "connectedAt": (datetime.now() - timedelta(days=40)),
            "isInProgress": True,
            "createdAt": datetime(2023, 11, 1),
            "updatedAt": datetime(2023, 11, 1),
        }
    )
    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    db_access.db_mongo_client[guildId]["guildmembers"].insert_one(
        {
            "discordId": "3451791",
            "username": "sample_user",
            "roles": ["99909821"],
            "joinedAt": (datetime.now() - timedelta(days=10)),
            "avatar": "3ddd6e429f75d6a711d0a58ba3060694",
            "isBot": False,
            "discriminator": "0",
        }
    )

    rawinfo_samples = []

    for i in range(150):
        sample = {
            "type": 0,
            "author": "3451791",
            "content": "test10",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"77776325{i}",
            "channelId": "41414262",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer()
    analyzer.recompute_analytics(guildId=guildId)

    memberactivities_data = db_access.db_mongo_client[guildId][
        "memberactivities"
    ].find_one({})
    heatmaps_data = db_access.db_mongo_client[guildId]["heatmaps"].find_one({})
    guild_document = db_access.db_mongo_client["Core"]["Platforms"].find_one(
        {"metadata.id": guildId}
    )

    # testing whether any data is available
    assert memberactivities_data is not None
    assert heatmaps_data is not None
    assert guild_document["isInProgress"] is False
