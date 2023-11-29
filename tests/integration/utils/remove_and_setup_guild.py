from datetime import datetime, timedelta
from typing import Optional

import numpy as np
from bson.objectid import ObjectId
from discord_analyzer.DB_operations.mongodb_access import DB_access


def setup_db_guild(
    db_access: DB_access,
    guildId: str = "1234",
    discordId_list: list[str] = ["973993299281076285"],
    discordId_isbot: list[bool] = [False],
    dates: Optional[list[datetime]] = None,
    days_ago_period: int = 30,
):
    """
    Remove the guild from Core databse and then insert it there
    also drop the guildId database and re-create
      it then create the guildmembers collection in it

    `discordId_isbot` is representative if each user is bot or not
    """
    platform_id = "515151515151515151515151"

    db_access.db_mongo_client["Core"]["Platforms"].delete_one(
        {"_id": ObjectId(platform_id)}
    )
    db_access.db_mongo_client.drop_database(guildId)

    db_access.db_mongo_client["Core"]["Platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guildId,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "selectedChannels": [
                    {"channelId": "1020707129214111827", "channelName": "general"}
                ],
                "window": [7, 1],
                "action": [1, 1, 1, 4, 3, 5, 5, 4, 3, 3, 2, 2, 1],
                "period": datetime.now() - timedelta(days=days_ago_period),
            },
            "community": ObjectId("aabbccddeeff001122334455"),
            "disconnectedAt": None,
            "connectedAt": (datetime.now() - timedelta(days=days_ago_period + 10)),
            "isInProgress": True,
            "createdAt": datetime(2023, 11, 1),
            "updatedAt": datetime(2023, 11, 1),
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
