from datetime import datetime, timedelta

from tc_analyzer_lib.automation.utils.automation_base import AutomationBase

from .utils.analyzer_setup import launch_db_access


def test_prepare_ngu_some_data_globalname_strategy():
    """
    test the preparation module in case of some data available
    the output should be have the names of the field `globalName`
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

    automation_base = AutomationBase()
    id_names = automation_base.prepare_names(
        guild_id=platform_id,
        user_ids=["1111", "1112", "1113", "1116"],
        user_field="globalName",
    )

    assert id_names == [
        (
            "1111",
            "User1GlobalName",
        ),
        ("1112", "User2GlobalName"),
        ("1113", None),
        ("1116", "User6GlobalName"),
    ]
