from bson.objectid import ObjectId
from utils.get_mongo_client import MongoSingleton


def get_guild_community_ids(platform_id: ObjectId) -> tuple[str, str]:
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

    platform = mongo_client["Core"]["platforms"].find_one(
        {"name": "discord", "_id": platform_id}
    )
    if platform is None:
        raise AttributeError(f"PLATFORM_ID: {platform_id}, No guild found!")

    guild_id = platform["metadata"]["id"]
    community_id = str(platform["community"])
    return guild_id, community_id
