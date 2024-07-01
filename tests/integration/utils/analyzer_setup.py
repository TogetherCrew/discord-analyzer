from discord_analyzer.DB_operations.mongodb_access import DB_access
from dotenv import load_dotenv


def launch_db_access(platform_id: str):
    load_dotenv()
    db_access = DB_access(platform_id)
    print("CONNECTED to MongoDB!")
    return db_access
