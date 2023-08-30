import os

from dotenv import load_dotenv

from discord_analyzer.DB_operations.mongodb_access import DB_access
from discord_analyzer.rn_analyzer import RnDaoAnalyzer


def setup_analyzer() -> RnDaoAnalyzer:
    load_dotenv()
    analyzer = RnDaoAnalyzer()

    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASS")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")

    neo4j_creds = {}
    neo4j_creds["db_name"] = os.getenv("NEO4J_DB")
    neo4j_creds["protocol"] = os.getenv("NEO4J_PROTOCOL")
    neo4j_creds["host"] = os.getenv("NEO4J_HOST")
    neo4j_creds["port"] = os.getenv("NEO4J_PORT")
    neo4j_creds["password"] = os.getenv("NEO4J_PASSWORD")
    neo4j_creds["user"] = os.getenv("NEO4J_USER")

    analyzer.set_mongo_database_info(
        mongo_db_host=host,
        mongo_db_password=password,
        mongo_db_user=user,
        mongo_db_port=port,
    )

    analyzer.set_neo4j_database_info(neo4j_creds=neo4j_creds)
    analyzer.database_connect()
    analyzer.setup_neo4j_metrics()

    return analyzer


def launch_db_access(guildId: str):
    load_dotenv()
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASS")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")

    connection_str = f"mongodb://{user}:{password}@{host}:{port}"

    db_access = DB_access(guildId, connection_str)
    print("CONNECTED to MongoDB!")
    return db_access
