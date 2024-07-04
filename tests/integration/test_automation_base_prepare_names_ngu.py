from datetime import datetime, timedelta

from tc_analyzer_lib.automation.utils.automation_base import AutomationBase

from .utils.analyzer_setup import launch_db_access


def test_prepare_ngu_no_data():
    """
    test the ngu preparation module in case of no data available
    the output should be an empty list
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("guildmembers")

    automation_base = AutomationBase()
    names = automation_base.prepare_names(guild_id=platform_id, user_ids=[])

    assert names == []


def test_prepare_ngu_some_data_ngu_strategy():
    """
    test the name preparation module in case of some data available
    the output should be have the names with the priority of ngu
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("guildmembers")

    db_access.db_mongo_client[platform_id]["guildmembers"].insert_many(
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
        ]
    )

    automation_base = AutomationBase()
    id_names = automation_base.prepare_names(
        guild_id=platform_id,
        user_ids=["1111", "1112", "1113", "1116", "1119"],
        user_field="ngu",
    )

    assert id_names == [
        ("1111", "User1NickName"),
        ("1112", "User2GlobalName"),
        ("1113", "user3"),
        ("1116", "User6NickName"),
        ("1119", "User9GlobalName"),
    ]
