from datetime import datetime, timedelta

from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_engagement_notifier_get_users_from_guildmembers_filled_collection():
    """
    test if the getting users from guildmembers correctly works
    in case of guildmembers collection having data
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
    user_data = notifier._get_users_from_guildmembers(
        guildId, user_ids=["1111", "1112", "1113", "1119"]
    )
    print(user_data)

    for user in user_data:
        if user["username"] == "user1":
            assert user["globalName"] == "User1GlobalName"
            assert user["nickname"] == "User1NickName"
        elif user["username"] == "user2":
            assert user["globalName"] == "User2GlobalName"
            assert user["nickname"] is None
        elif user["username"] == "user3":
            assert user["globalName"] is None
            assert user["nickname"] is None
        elif user["username"] == "user9":
            assert user["globalName"] == "User9GlobalName"
            assert user["nickname"] is None
        else:
            # we have to never reach here
            # as we have not queried the "user6"
            assert False is True


def test_engagement_notifier_get_users_from_guildmembers_no_data():
    """
    test if the getting users from guildmembers correctly works
    in case of guildmembers collection having data
    """
    guildId = "1234"
    db_access = launch_db_access(guildId)

    db_access.db_mongo_client[guildId].drop_collection("guildmembers")
    db_access.db_mongo_client[guildId]["guildmembers"].delete_many({})

    db_access.db_mongo_client[guildId].drop_collection("guildmembers")
    db_access.db_mongo_client[guildId]["guildmembers"].delete_many({})

    notifier = EngagementNotifier()
    user_data = notifier._get_users_from_guildmembers(
        guildId, user_ids=["1111", "1112", "1113", "1119"]
    )

    assert user_data == []
