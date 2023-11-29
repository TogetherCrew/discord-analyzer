# test analyzing memberactivities
from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.remove_and_setup_guild import setup_db_guild


def test_analyzer_member_activities_from_start_available_member_activity():
    """
    run the analyzer for a specific guild with from_start option equal to True
    assuming the memberactivities collection is empty
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    setup_db_guild(db_access, guildId, discordId_list=["973993299281076285"])

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    memberactivity_data = create_empty_memberactivities_data(
        datetime(year=2023, month=6, day=5)
    )
    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        memberactivity_data
    )

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
