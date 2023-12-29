from bson.objectid import ObjectId
from utils.get_mongo_client import MongoSingleton


def get_guild_community_ids(platform_id: str) -> tuple[str, str]:
    """
    get both the guild id and community from the platform id

    Parameters
    -----------
    platform_id : str
        the platform `_id` within the platforms collection

    Returns
    --------
    guild_id : str
        the discord guild id for that specific platform
    community_id : str
        the community id that the guild is related
    """
    mongo_client = MongoSingleton.get_instance().client

    obj_platform_id = ObjectId(platform_id)
    platform = mongo_client["Core"]["platforms"].find_one(
        {"name": "discord", "_id": obj_platform_id}
    )
    if platform is None:
        raise AttributeError(f"PLATFORM_ID: {platform_id}, No guild found!")

    guild_id = platform["metadata"]["id"]
    community_id = str(platform["community"])
    return guild_id, community_id


def get_guild_platform_id(guild_id: str) -> str:
    """
    get the guild platform id using the given guild id

    Parameters
    ------------
    guild_id : str
        the id for the specified guild

    Returns
    --------
    platform_id : str
        the platform id related to the given guild
    """
    mongo_client = MongoSingleton.get_instance().client

    guild_info = mongo_client["Core"]["platforms"].find_one(
        {"metadata.id": guild_id}, {"_id": 1}
    )
    if guild_info is not None:
        platform_id = str(guild_info["_id"])
    else:
        raise ValueError(f"No available guild with id {guild_id}")

    return platform_id
