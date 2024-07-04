import os
from datetime import datetime, timedelta

from bson.objectid import ObjectId
from dotenv import load_dotenv
from tc_analyzer_lib.automation.utils.interfaces import (
    Automation,
    AutomationAction,
    AutomationReport,
    AutomationTrigger,
)
from tc_analyzer_lib.publish_on_success import publish_on_success

from .utils.analyzer_setup import launch_db_access


def test_publish_on_success_recompute_true_check_notification_choreographies():
    """
    test the publish on success functions
    we want to check the database if the notify choreographies are created
    """
    load_dotenv()
    platform_id = "515151515151515151515151"
    guild_id = "1234"
    saga_id = "000000011111113333377777ie0w"
    expected_owner_id = "334461287892"
    db_access = launch_db_access(guild_id)
    at_db = os.getenv("AUTOMATION_DB_NAME")
    at_collection = os.getenv("AUTOMATION_DB_COLLECTION")

    db_access.db_mongo_client["Core"].drop_collection("platforms")
    db_access.db_mongo_client["Core"].drop_collection("users")
    db_access.db_mongo_client.drop_database(platform_id)
    db_access.db_mongo_client.drop_database(guild_id)
    db_access.db_mongo_client["Saga"].drop_collection("sagas")
    db_access.db_mongo_client[at_db].drop_collection(at_collection)

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
    community_id = "aabbccddeeff001122334455"
    owner_discord_id = "123487878912"

    db_access.db_mongo_client["Core"]["platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guild_id,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "resources": ["4455178"],
                "window": window,
                "action": act_param,
                "period": datetime.now() - timedelta(days=10),
            },
            "community": ObjectId(community_id),
            "disconnectedAt": None,
            "connectedAt": (datetime.now() - timedelta(days=10)),
            "isInProgress": True,
            "createdAt": datetime(2023, 11, 1),
            "updatedAt": datetime(2023, 11, 1),
        }
    )

    db_access.db_mongo_client["Core"]["users"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "discordId": owner_discord_id,
            "communities": [ObjectId(community_id)],
            "createdAt": datetime(2023, 12, 1),
            "updatedAt": datetime(2023, 12, 1),
            "tcaAt": datetime(2023, 12, 2),
        }
    )

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
                "platformId": platform_id,
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

    db_access.db_mongo_client[guild_id]["guildmembers"].insert_many(
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
                "nickname": "User1NickName",  # this will be used for the message
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
                "globalName": "User2GlobalName",  # this will be used for the message
                "nickname": None,
            },
            {
                "discordId": "1113",
                "username": "user3",  # this will be used for the message
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
            {
                "discordId": "999",
                "username": "community_manager",
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

    triggers = [
        AutomationTrigger(options={"category": "all_new_disengaged"}, enabled=True),
        AutomationTrigger(options={"category": "all_new_active"}, enabled=False),
    ]
    actions = [
        AutomationAction(
            template="hey {{ngu}}! please get back to us!",
            options={},
            enabled=True,
        ),
        AutomationAction(
            template="hey {{ngu}}! please get back to us2!",
            options={},
            enabled=False,
        ),
    ]

    report = AutomationReport(
        recipientIds=["999"],
        template="hey body! This users were messaged:\n{{#each usernames}}{{this}}{{/each}}",
        options={},
        enabled=True,
    )
    today_time = datetime.now()

    automation = Automation(
        guild_id,
        triggers,
        actions,
        report,
        enabled=True,
        createdAt=today_time,
        updatedAt=today_time,
    )

    db_access.db_mongo_client[at_db][at_collection].insert_one(automation.to_dict())

    date_yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    date_two_past_days = (datetime.now() - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
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

    publish_on_success(platform_id, recompute=True)

    notification_count = db_access.db_mongo_client["Saga"]["sagas"].count_documents(
        {"choreography.name": "DISCORD_NOTIFY_USERS"}
    )

    assert notification_count == 5

    user1_doc = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"data.discordId": "1111"}
    )
    assert user1_doc["data"]["message"] == ("hey User1NickName! please get back to us!")

    user2_doc = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"data.discordId": "1112"}
    )
    assert user2_doc["data"]["message"] == (
        "hey User2GlobalName! please get back to us!"
    )

    user3_doc = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"data.discordId": "1113"}
    )
    assert user3_doc["data"]["message"] == ("hey user3! please get back to us!")

    user_cm_doc = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"data.discordId": "999"}
    )
    expected_msg = "hey body! This users were messaged:\n"
    expected_msg += "- User1NickName\n- User2GlobalName\n- user3\n"
    assert user_cm_doc["data"]["message"] == expected_msg

    job_finished_saga = db_access.db_mongo_client["Saga"]["sagas"].find_one(
        {"data.discordId": owner_discord_id}
    )
    assert job_finished_saga["data"]["message"] == (
        "Your data import into TogetherCrew is complete! "
        "See your insights on your dashboard https://app.togethercrew.com/."
        " If you have questions send a DM to katerinabc (Discord) or k_bc0 (Telegram)."
    )
