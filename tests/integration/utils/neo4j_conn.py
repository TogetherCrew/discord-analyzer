import os

from dotenv import load_dotenv
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def neo4j_setup() -> Neo4jOps:
    load_dotenv()

    protocol = os.getenv("NEO4J_PROTOCOL")
    host = os.getenv("NEO4J_HOST")
    port = os.getenv("NEO4J_PORT")
    db_name = os.getenv("NEO4J_DB")

    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    neo4j_ops = Neo4jOps()
    neo4j_ops.set_neo4j_db_info(
        neo4j_db_name=db_name,
        neo4j_protocol=protocol,
        neo4j_user=user,
        neo4j_password=password,
        neo4j_host=host,
        neo4j_port=port,
    )
    neo4j_ops.neo4j_database_connect()

    return neo4j_ops
