import os

from discord_analyzer.DB_operations.mongodb_access import DB_access
from discord_analyzer.tc_analyzer import TCAnalyzer
from dotenv import load_dotenv


def setup_analyzer(
    guild_id: str,
) -> TCAnalyzer:
    load_dotenv()

    analyzer = TCAnalyzer(guild_id)
    analyzer.database_connect()

    return analyzer


def launch_db_access(platform_id: str):
    load_dotenv()
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASS")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")

    connection_str = f"mongodb://{user}:{password}@{host}:{port}"

    db_access = DB_access(platform_id, connection_str)
    print("CONNECTED to MongoDB!")
    return db_access
