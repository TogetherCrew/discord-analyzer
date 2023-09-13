# test analyzing memberactivities
from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_analyzer_from_start_one_interval():
    """
    run the analyzer from start and just for one interval
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    setup_db_guild(db_access, guildId, discordId_list=["973993299281076285"])

    rawinfo_samples = []

    for i in range(150):
        sample = {
            "type": 0,
            "author": "973993299281076285",
            "content": "test10",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"11188143219343360{i}",
            "channelId": "1020707129214111827",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

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
