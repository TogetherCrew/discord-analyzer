# test analyzing memberactivities
from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access, setup_analyzer


def test_analyzer_member_activities_from_start_empty_memberactivities():
    """
    run the analyzer for a specific guild with from_start option equal to True
    assuming the memberactivities collection is empty
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client["RnDAO"]["guilds"].delete_one({"guildId": guildId})
    db_access.db_mongo_client.drop_database(guildId)

    db_access.db_mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guildId,
            "user": "1223455",
            "name": "Loud place",
            "connectedAt": (datetime.now() - timedelta(days=10)),
            "isInProgress": True,
            "isDisconnected": False,
            "icon": "afd0d06fd12b2905c53708ca742e6c66",
            "window": [7, 1],
            "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 3, 2, 2, 1],
            "selectedChannels": [
                {
                    "channelId": "41414262",
                    "channelName": "general",
                },
            ],
            "period": (datetime.now() - timedelta(days=30)),
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
            "IsGeneratedByWebhook": False,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer()
    analyzer.recompute_analytics(guildId=guildId)

    memberactivities_data = db_access.db_mongo_client[guildId][
        "memberactivities"
    ].find_one({})
    heatmaps_data = db_access.db_mongo_client[guildId]["heatmaps"].find_one({})
    guild_document = db_access.db_mongo_client["RnDAO"]["guilds"].find_one(
        {"guildId": guildId}
    )

    # testing whether any data is available
    assert memberactivities_data is not None
    assert heatmaps_data is not None
    assert guild_document["isInProgress"] is False
