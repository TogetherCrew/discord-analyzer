from datetime import datetime, timedelta

from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_engagement_notifier_fire_message_check_mongodb_document_count():
    """
    check the saga count created
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client[guildId].drop_collection("memberactivities")
    db_access.db_mongo_client[guildId]["memberactivities"].delete_many({})

    db_access.db_mongo_client["Saga"].drop_collection("sagas")
    db_access.db_mongo_client["Saga"]["sagas"].delete_many({})

    db_access.db_mongo_client[guildId].drop_collection("guildmembers")
    db_access.db_mongo_client[guildId]["guildmembers"].delete_many({})

    db_access.db_mongo_client["RnDAO"].drop_collection("guilds")

    db_access.db_mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guildId,
            "user": "owner_id",
            "name": "Sample Guild",
            "connectedAt": datetime.now(),
            "isInProgress": False,
            "isDisconnected": False,
            "icon": "4256asdiqwjo032",
            "window": [7, 1],
            "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 2, 2, 2, 1],
            "selectedChannels": [
                {
                    "channelId": "11111111111111",
                    "channelName": "general",
                },
            ],
        }
    )

    db_access.db_mongo_client[guildId]["guildmembers"].insert_many(
        [
            {
                "discordId": "1111",
                "username": "user1",
                "roles": [],
                "joinedAt": datetime.now() - timedelta(days=10),
                "avatar": None,
                "isBot": False,
                "discriminator": "0",
                "permissions": "6677",
                "deletedAt": None,
                "globalName": "User1GlobalName",
                "nickname": "User1NickName",
            },
            {
                "discordId": "1112",
                "username": "user2",
                "roles": [],
                "joinedAt": datetime.now() - timedelta(days=10),
                "avatar": None,
                "isBot": False,
                "discriminator": "0",
                "permissions": "6677",
                "deletedAt": None,
                "globalName": "User2GlobalName",
                "nickname": None,
            },
            {
                "discordId": "1113",
                "username": "user3",
                "roles": [],
                "joinedAt": datetime.now() - timedelta(days=10),
                "avatar": None,
                "isBot": False,
                "discriminator": "0",
                "permissions": "6677",
                "deletedAt": None,
                "globalName": None,
                "nickname": None,
            },
            {
                "discordId": "1116",
                "username": "user6",
                "roles": [],
                "joinedAt": datetime.now() - timedelta(days=10),
                "avatar": None,
                "isBot": False,
                "discriminator": "0",
                "permissions": "6677",
                "deletedAt": None,
                "globalName": "User6GlobalName",
                "nickname": "User6NickName",
            },
            {
                "discordId": "1119",
                "username": "user9",
                "roles": [],
                "joinedAt": datetime.now() - timedelta(days=10),
                "avatar": None,
                "isBot": False,
                "discriminator": "0",
                "permissions": "6677",
                "deletedAt": None,
                "globalName": "User9GlobalName",
                "nickname": None,
            },
        ]
    )

    db_access.db_mongo_client["Saga"].drop_collection("sagas")

    date_yesterday = (
        (datetime.now() - timedelta(days=1))
        .replace(hour=0, minute=0, second=0)
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    date_two_past_days = (
        (datetime.now() - timedelta(days=2))
        .replace(hour=0, minute=0, second=0)
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        [
            {
                "date": date_yesterday,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["1111", "1112", "1113"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
            {
                "date": date_two_past_days,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["1116", "1119"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
        ]
    )

    notifier = EngagementNotifier()
    notifier.notify_disengaged(guildId)

    # sending messages to 4 users (1111, 1112, 1113, and owner)
    doc_count = db_access.db_mongo_client["Saga"]["sagas"].count_documents({})
    assert doc_count == 4
