from typing import Any

from discord_analyzer.DB_operations.mongo_neo4j_ops import MongoNeo4jDB


class AnalyzerDBManager:
    def __init__(self):
        """
        base class for the analyzer
        """
        self.connection_str = None

    def set_mongo_database_info(
        self,
        mongo_db_user: str,
        mongo_db_password: str,
        mongo_db_host: str,
        mongo_db_port: str,
    ):
        """
        MongoDB Database information setter
        """
        self.mongo_user = mongo_db_user
        self.mongo_pass = mongo_db_password
        self.mongo_host = mongo_db_host
        self.mongo_port = mongo_db_port

        self.connection_str = f"mongodb://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}:{self.mongo_port}"

    def database_connect(self):
        """
        Connect to the database
        """
        """ Connection String will be modified once the url is provided"""

        self.DB_connections = MongoNeo4jDB(testing=False)
        self.DB_connections.set_mongo_db_ops(
            mongo_user=self.mongo_user,
            mongo_pass=self.mongo_pass,
            mongo_host=self.mongo_host,
            mongo_port=self.mongo_port,
        )
