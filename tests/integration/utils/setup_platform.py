from datetime import datetime, timedelta
from typing import Optional

import numpy as np
from bson.objectid import ObjectId
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access
from tc_analyzer_lib.tc_analyzer import TCAnalyzer


def setup_platform(
    db_access: DB_access,
    platform_id: str,
    discordId_list: list[str] = ["973993299281076285"],
    discordId_isbot: list[bool] = [False],
    dates: Optional[list[datetime]] = None,
    days_ago_period: int = 30,
    **kwargs,
) -> TCAnalyzer:
    """
    Remove the guild from Core databse and then insert it there
    also drop the guildId database and re-create
      it then create the rawmembers collection in it

    `discordId_isbot` is representative if each user is bot or not
    `community_id` can be passed in kwargs. default is `aabbccddeeff001122334455`
    """
    community_id = kwargs.get("community_id", "aabbccddeeff001122334455")
    resources = kwargs.get("resources", ["1020707129214111827"])
    db_access.db_mongo_client["Core"]["platforms"].delete_one(
        {"_id": ObjectId(platform_id)}
    )
    db_access.db_mongo_client.drop_database(platform_id)

    period = (datetime.now() - timedelta(days=days_ago_period)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    window = kwargs.get(
        "window",
        {"period_size": 7, "step_size": 1},
    )

    action = kwargs.get(
        "action",
        {
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
        },
    )

    guildId = "1234"
    db_access.db_mongo_client["Core"]["platforms"].insert_one(
        {
            "_id": ObjectId(platform_id),
            "name": "discord",
            "metadata": {
                "id": guildId,
                "icon": "111111111111111111111111",
                "name": "A guild",
                "resources": resources,
                "window": window,
                "action": action,
                "period": period,
            },
            "community": ObjectId(community_id),
            "disconnectedAt": None,
            "connectedAt": (datetime.now() - timedelta(days=days_ago_period + 10)),
            "isInProgress": True,
            "createdAt": datetime(2023, 11, 1),
            "updatedAt": datetime(2023, 11, 1),
        }
    )

    analyzer = TCAnalyzer(
        platform_id,
        resources=resources,
        period=period,
        action=action,
        window=window,
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
        db_access.db_mongo_client[platform_id]["rawmembers"].insert_one(
            {
                "id": discordId,
                "joined_at": dates_using[idx],
                "left_at": None,
                "is_bot": isbot,
                "options": {},
            }
        )

    return analyzer
