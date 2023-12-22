import os

from discord_analyzer import RnDaoAnalyzer
from dotenv import load_dotenv


def test_mongo_db_info_set():
    community_id = "4321"
    analyzer = RnDaoAnalyzer(community_id)
    load_dotenv()

    port = 1234
    host = "http://www.google.com"
    # to ignore gitleaks
    password = os.getenv("MONGODB_PASS")
    user = "sample_user"

    analyzer.set_mongo_database_info(
        mongo_db_host=host,
        mongo_db_password=password,
        mongo_db_user=user,
        mongo_db_port=port,
    )
    assert analyzer.mongo_host == host
    assert analyzer.mongo_pass == password
    assert analyzer.mongo_user == user
    assert analyzer.mongo_port == port


def test_neo4j_db_info_set():
    load_dotenv()
    port = 1234
    db_name = "db"
    protocol = "bolt"
    user = "user"
    host = "localhost"
    # to ignore gitleaks
    password = os.getenv("NEO4J_PASSWORD")
    neo4j_creds = {
        "db_name": db_name,
        "password": password,
        "port": port,
        "protocol": protocol,
        "host": host,
        "user": user,
    }

    community_id = "4321"
    analyzer = RnDaoAnalyzer(community_id)
    analyzer.set_neo4j_database_info(neo4j_creds=neo4j_creds)

    assert analyzer.neo4j_port == port
    assert analyzer.neo4j_host == host
    assert analyzer.neo4j_protocol == protocol
    assert analyzer.neo4j_db_name == db_name
    assert analyzer.neo4j_password == password
    assert analyzer.neo4j_user == user
