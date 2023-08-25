import os

from dotenv import load_dotenv

from discord_analyzer.DB_operations.neo4j_utils import Neo4jUtils


def neo4j_setup() -> Neo4jUtils:
    load_dotenv()

    protocol = os.getenv("NEO4J_PROTOCOL")
    host = os.getenv("NEO4J_HOST")
    port = os.getenv("NEO4J_PORT")
    db_name = os.getenv("NEO4J_DB")

    url = f"{protocol}://{host}:{port}"
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    neo4j_utils = Neo4jUtils()
    neo4j_utils.set_neo4j_db_info(
        neo4j_db_name=db_name, neo4j_url=url, neo4j_user=user, neo4j_password=password
    )
    neo4j_utils.neo4j_database_connect()

    return neo4j_utils
