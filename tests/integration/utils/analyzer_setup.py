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
    db_access = DB_access(platform_id)
    print("CONNECTED to MongoDB!")
    return db_access
