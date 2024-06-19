from typing import Any

from discord_analyzer.tc_analyzer import TCAnalyzer
from utils.credentials import get_mongo_credentials


class AnalyzerInit:
    """
    initialize the analyzer with its configs
    """

    # TODO: update to platform_id as input
    def __init__(self, guild_id: str) -> None:
        self.guild_id = guild_id

    def get_analyzer(self) -> TCAnalyzer:
        """
        Returns:
        ---------
        analyzer : TCAnalyzer
        """
        analyzer = TCAnalyzer(self.guild_id)

        # credentials
        mongo_creds = get_mongo_credentials()

        analyzer.set_mongo_database_info(
            mongo_db_host=mongo_creds["host"],
            mongo_db_password=mongo_creds["password"],
            mongo_db_port=mongo_creds["port"],
            mongo_db_user=mongo_creds["user"],
        )
        analyzer.database_connect()

        return analyzer

    def _get_mongo_connection(self, mongo_creds: dict[str, Any]):
        user = mongo_creds["user"]
        password = mongo_creds["password"]
        host = mongo_creds["host"]
        port = mongo_creds["port"]

        connection = f"mongodb://{user}:{password}@{host}:{port}"

        return connection
