from dotenv import load_dotenv
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access


def launch_db_access(platform_id: str):
    load_dotenv()
    db_access = DB_access(platform_id)
    print("CONNECTED to MongoDB!")
    return db_access
