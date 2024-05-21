from typing import Any

from discord_analyzer import RnDaoAnalyzer
from utils.credentials import get_mongo_credentials, get_neo4j_credentials


class AnalyzerInit:
    """
    initialize the analyzer with its configs
    """

    # TODO: update to platform_id as input
    def __init__(self, guild_id: str) -> None:
        self.guild_id = guild_id

    def get_analyzer(self) -> RnDaoAnalyzer:
        """
        Returns:
        ---------
        analyzer : RnDaoAnalyzer
        """
        analyzer = RnDaoAnalyzer(self.guild_id)

        # credentials
        mongo_creds = get_mongo_credentials()
        neo4j_creds = get_neo4j_credentials()

        analyzer.set_mongo_database_info(
            mongo_db_host=mongo_creds["host"],
            mongo_db_password=mongo_creds["password"],
            mongo_db_port=mongo_creds["port"],
            mongo_db_user=mongo_creds["user"],
        )
        analyzer.set_neo4j_database_info(neo4j_creds=neo4j_creds)
        analyzer.database_connect()
        analyzer.setup_neo4j_metrics()

        return analyzer

    def _get_mongo_connection(self, mongo_creds: dict[str, Any]):
        user = mongo_creds["user"]
        password = mongo_creds["password"]
        host = mongo_creds["host"]
        port = mongo_creds["port"]

        connection = f"mongodb://{user}:{password}@{host}:{port}"

        return connection
