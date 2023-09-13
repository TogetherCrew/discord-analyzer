from discord_analyzer.schemas.accounts import AccountCounts
from pymongo import MongoClient


def store_counts_dict(counts_dict):
    # make empty result array
    obj_array = []

    # for each account
    for acc in counts_dict.keys():
        # make dict and store in array
        obj_array.append(AccountCounts(acc, counts_dict[acc]).asdict())

    return obj_array


def getNumberOfActions(heatmap):
    """get number of actions"""
    sum_ac = 0
    fields = [
        "thr_messages",
        "lone_messages",
        "replier",
        "replied",
        "mentioned",
        "mentioner",
        "reacter",
        "reacted",
    ]
    for field in fields:
        for i in range(24):
            sum_ac += heatmap[field][i]
    return sum_ac


def get_bot_id(
    db_mongo_client: MongoClient,
    guildId: str,
    collection_name: str = "guildmembers",
    id_field_name: str = "discordId",
) -> list[str]:
    """
    get the bot id from guildmembers collection

    Parameters:
    ------------
    db_mongo_client : MongoClient
        the access to database
    guildId : str
        the guildId to connect to
    collection_name : str
        the collection name to use
        default is "guildmembers"
    id_field_name : str
        the fieldId that the account id is saved
        default is "discordId"

    Returns:
    ---------
    bots : list[str]
        the list of bot ids
    """
    cursor = db_mongo_client[guildId][collection_name].find(
        {"isBot": True}, {"_id": 0, id_field_name: 1}
    )
    bots = list(cursor)

    bot_ids = []
    if bots != []:
        bot_ids = list(map(lambda x: x[id_field_name], bots))

    return bot_ids
