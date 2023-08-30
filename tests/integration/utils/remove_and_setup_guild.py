from datetime import datetime, timedelta

import numpy as np
from discord_analyzer.DB_operations.mongodb_access import DB_access


def setup_db_guild(
    db_access: DB_access,
    guildId: str = "1234",
    discordId_list: list[str] = ["973993299281076285"],
    discordId_isbot: list[bool] = [False],
    dates: list[datetime] = None,
    days_ago_period: int = 30,
):
    """
    Remove the guild from RnDAO databse and then insert it there
    also drop the guildId database and re-create
      it then create the guildmembers collection in it

    `discordId_isbot` is representative if each user is bot or not
    """

    db_access.db_mongo_client["RnDAO"]["guilds"].delete_one({"guildId": guildId})
    db_access.db_mongo_client.drop_database(guildId)

    db_access.db_mongo_client["RnDAO"]["guilds"].insert_one(
        {
            "guildId": guildId,
            "user": "876487027099582524",
            "name": "Sample Guild",
            "connectedAt": (datetime.now() - timedelta(days=10)),
            "isInProgress": True,
            "isDisconnected": False,
            "icon": "afd0d06fd12b2905c53708ca742e6c66",
            "window": [7, 1],
            "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 3, 2, 2, 1],
            "selectedChannels": [
                {
                    "channelId": "1020707129214111827",
                    "channelName": "general",
                },
            ],
            "period": (datetime.now() - timedelta(days=days_ago_period)),
        }
    )

    if dates is None:
        dates_using = np.repeat(
            datetime.now() - timedelta(days=10), len(discordId_list)
        )
    else:
        dates_using = dates

    # just to create the data we're inserting one by one
    # it's not the most efficient way

    # if the isBot parameters was not set
    # set all the users to not to be a bot
    if len(discordId_isbot) != len(discordId_list):
        user_data = zip(discordId_list, [False] * len(discordId_list))
    else:
        user_data = zip(discordId_list, discordId_isbot)

    for idx, (discordId, isbot) in enumerate(user_data):
        db_access.db_mongo_client[guildId]["guildmembers"].insert_one(
            {
                "discordId": discordId,
                "username": f"sample_user_{idx}",
                "roles": ["1012430565959553145"],
                "joinedAt": dates_using[idx],
                "avatar": "3ddd6e429f75d6a711d0a58ba3060694",
                "isBot": isbot,
                "discriminator": "0",
            }
        )
