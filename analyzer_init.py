from typing import Any

from discord_analyzer.tc_analyzer import TCAnalyzer


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
        analyzer.database_connect()

        return analyzer

    def _get_mongo_connection(self, mongo_creds: dict[str, Any]):
        user = mongo_creds["user"]
        password = mongo_creds["password"]
        host = mongo_creds["host"]
        port = mongo_creds["port"]

        connection = f"mongodb://{user}:{password}@{host}:{port}"

        return connection
