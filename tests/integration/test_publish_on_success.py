import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

from discord_utils import publish_on_success

from .utils.analyzer_setup import launch_db_access


def test_publish_on_success_check_notification_choreographies():
    """
    test the publish on success functions
    we want to check the database if the notify choreographies are created
    """
    load_dotenv()

    guildId = "915914985140531240"
    saga_id = "000000011111113333377777ie0w"
    expected_owner_id = "334461287892"
    db_access = launch_db_access(guildId)
    db_access.db_mongo_client["RnDAO"].drop_collection("guilds")

    db_access.db_mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guildId,
            "user": expected_owner_id,
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

    db_access.db_mongo_client[guildId].drop_collection("guildmembers")
    db_access.db_mongo_client[guildId]["guildmembers"].delete_many({})

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
        ]
    )

    db_access.db_mongo_client[guildId].drop_collection("memberactivities")
    db_access.db_mongo_client[guildId]["memberactivities"].delete_many({})

    # Adding sample memberactivities
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
                "all_new_disengaged": ["1111", "1112"],
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
                "all_new_disengaged": ["user3", "user6", "user9"],
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

    # Adding a sample saga
    db_access.db_mongo_client["Saga"].drop_collection("sagas")

    db_access.db_mongo_client["Saga"]["sagas"].insert_one(
        {
            "choreography": {
                "name": "DISCORD_UPDATE_CHANNELS",
                "transactions": [
                    {
                        "queue": "DISCORD_BOT",
                        "event": "FETCH",
                        "order": 1,
                        "status": "SUCCESS",
                        "start": datetime.now(),
                        "end": datetime.now(),
                        "runtime": 1,
                    },
                    {
                        "queue": "DISCORD_ANALYZER",
                        "event": "RUN",
                        "order": 2,
                        "status": "SUCCESS",
                        "start": datetime.now(),
                        "end": datetime.now(),
                        "runtime": 1,
                    },
                ],
            },
            "status": "IN_PROGRESS",
            "data": {
                "guildId": guildId,
                "created": False,
                "discordId": expected_owner_id,
                "message": "data is ready",
                "useFallback": True,
            },
            "sagaId": saga_id,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now(),
        }
    )

    # these args data were made from the rabbitMQ parameters
    mongo_creds = {
        "connection_str": "mongodb://admin:password@localhost:27017/",
        "db_name": os.getenv("SAGA_DB_NAME"),
        "collection_name": os.getenv("SAGA_DB_COLLECTION"),
    }

    sample_args_data = ["sample", saga_id, mongo_creds]
    publish_on_success(None, None, sample_args_data)

    cursor = db_access.db_mongo_client["Saga"]["sagas"].find(
        {"choreography.name": "DISCORD_NOTIFY_USERS"}
    )

    notification_sagas = list(cursor)
    # two notified users
    # and one notified guild owner
    assert len(notification_sagas) == 3

    for saga in notification_sagas:
        assert saga["data"]["discordId"] in ["1111", "1112", expected_owner_id]
        # the owner of the guild receives a different message
        if saga["data"]["discordId"] == expected_owner_id:
            expected_message = "The following members disengaged and were messaged:\n"
            expected_message += "- User1NickName\n"
            expected_message += "- User2GlobalName\n"

            saga["data"]["message"] == expected_message
