from datetime import datetime

from engagement_notifier.engagement import EngagementNotifier

from .utils.analyzer_setup import launch_db_access


def test_engagement_notifier_guild_owner_no_guild_info():
    """
    test if there's no guild info was available the output should be None
    """
    guild_id = "1234"
    expected_owner_id = "12345678901234567"
    db_access = launch_db_access(guild_id)

    db_access.db_mongo_client["RnDAO"].drop_collection("guilds")
    db_access.db_mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guild_id,
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

    engagement_notifier = EngagementNotifier()

    owner_id = engagement_notifier.get_owner_id(guild_id=guild_id)

    assert owner_id == expected_owner_id
