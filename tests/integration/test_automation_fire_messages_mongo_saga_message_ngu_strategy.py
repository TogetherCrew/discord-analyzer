from datetime import datetime, timedelta

from tc_analyzer_lib.automation.automation_workflow import AutomationWorkflow
from tc_analyzer_lib.automation.utils.interfaces import (
    Automation,
    AutomationAction,
    AutomationReport,
    AutomationTrigger,
)

from .utils.analyzer_setup import launch_db_access


def test_automation_fire_message_check_mongodb_document_messages_ngu_strategy():
    """
    check the created messages in saga
    """
    guild_id = "1234"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")
    db_access.db_mongo_client["Saga"].drop_collection("sagas")
    db_access.db_mongo_client[guild_id].drop_collection("guildmembers")
    db_access.db_mongo_client["Automation"].drop_collection("automations")

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

    db_access.db_mongo_client["Automation"]["automations"].insert_one(
        automation.to_dict()
    )

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

    automation_workflow = AutomationWorkflow()
    automation_workflow.start(platform_id, guild_id)

    count = db_access.db_mongo_client["Saga"]["sagas"].count_documents({})
    assert count == 4

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
