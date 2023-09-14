from datetime import datetime, timedelta

from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_prepare_ngu_no_data():
    """
    test the ngu preparation module in case of no data available
    the output should be an empty list
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client[guildId].drop_collection("guildmembers")
    db_access.db_mongo_client[guildId]["guildmembers"].delete_many({})

    notifier = EngagementNotifier()
    names = notifier.prepare_names(guild_id=guildId, user_ids=[])

    assert names == []


def test_prepare_ngu_some_data():
    """
    test the ngu preparation module in case of some data available
    the output should be have the names with the priority of ngu
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)

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

    notifier = EngagementNotifier()
    names = notifier.prepare_names(
        guild_id=guildId, user_ids=["1111", "1112", "1113", "1116", "1119"]
    )

    assert set(names) == set(
        [
            "User1NickName",
            "User2GlobalName",
            "user3",
            "User6NickName",
            "User9GlobalName",
        ]
    )
