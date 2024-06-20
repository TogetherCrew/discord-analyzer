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

    db_access.db_mongo_client["Core"]["platforms"].delete_one({"metadata.id": guildId})
    db_access.db_mongo_client.drop_database(guildId)

    action = {
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

    db_access.db_mongo_client["Core"]["platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guildId,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "resources": ["1020707129214111827"],
                "window": {"period_size": 7, "step_size": 1},
                "action": action,
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
    db_access.db_mongo_client[platform_id].create_collection("heatmaps")
    db_access.db_mongo_client[platform_id].create_collection("memberactivities")

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

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
        rawinfo_samples
    )

    analyzer = setup_analyzer(platform_id)
    analyzer.recompute_analytics()

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
