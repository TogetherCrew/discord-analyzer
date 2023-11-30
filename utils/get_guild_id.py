from bson.objectid import ObjectId
from utils.get_mongo_client import MongoSingleton


def get_guild_id(platform_id: ObjectId) -> str:
    """
    get the guild id from the platform id

    Parameters
    -----------
    platform_id : str
        the platform `_id` within the Platforms collection

    Returns
    --------
    guild_id : str
        the discord guild id for that specific platform
    """
    mongo_client = MongoSingleton.get_instance().client

    platform = mongo_client["Core"]["Platforms"].find_one(
        {"name": "discord", "_id": platform_id}
    )
    if platform is None:
        raise AttributeError("No guild found!")

    guild_id = platform["metadata"]["id"]
    return guild_id
