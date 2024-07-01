# test analyzing memberactivities
from datetime import datetime, timedelta

from bson.objectid import ObjectId

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_analyzer_member_activities_from_start_empty_memberactivities():
    """
    run the analyzer for a specific guild with from_start option equal to True
    assuming the memberactivities collection is empty
    """
    # first create the collections
    guildId = "1234"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client["Core"].drop_collection("platforms")
    db_access.db_mongo_client.drop_database(platform_id)

    analyzer = setup_platform(
        db_access,
        platform_id,
        discordId_list=["3451791"],
        days_ago_period=30,
        community_id="aabbccddeeff001122334455",
    )
    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    db_access.db_mongo_client[platform_id]["rawmembers"].insert_one(
        {
            "id": "3451791",
            "joined_at": (datetime.now() - timedelta(days=10)),
            "left_at": None,
            "is_bot": False,
            "options": {},
        }
    )

    rawinfo_samples = []

    for i in range(150):
        author = "3451791"
        samples = [
            {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [],
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

    analyzer.recompute()

    memberactivities_data = db_access.db_mongo_client[platform_id][
        "memberactivities"
    ].find_one({})
    heatmaps_data = db_access.db_mongo_client[platform_id]["heatmaps"].find_one({})
    guild_document = db_access.db_mongo_client["Core"]["platforms"].find_one(
        {"metadata.id": guildId}
    )

    # testing whether any data is available
    assert memberactivities_data is not None
    assert heatmaps_data is not None
    assert guild_document["metadata"]["isInProgress"] is False
