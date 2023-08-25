import logging
from typing import Any

from discord_analyzer.DB_operations.mongo_neo4j_ops import MongoNeo4jDB  # isort: skip


class Base_analyzer:
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

    def set_neo4j_database_info(self, neo4j_creds: dict[str, Any]):
        """
        set neo4J database informations

        Parameters:
        -------------
        neo4j_creds : dict[str, Any]
            neo4j_credentials to connect
            the keys should be
            - db_name: str
            - protocol: str
            - host: str
            - port: int
            - user: str
            - password: str
        """
        self.neo4j_db_name = neo4j_creds["db_name"]
        self.neo4j_protocol = neo4j_creds["protocol"]
        self.neo4j_host = neo4j_creds["host"]
        self.neo4j_port = neo4j_creds["port"]
        self.neo4j_user = neo4j_creds["user"]
        self.neo4j_password = neo4j_creds["password"]

    def database_connect(self):
        """
        Connect to the database
        """
        """ Connection String will be modified once the url is provided"""

        self.DB_connections = MongoNeo4jDB(logging=logging, testing=False)
        self.DB_connections.set_mongo_db_ops(
            mongo_user=self.mongo_user,
            mongo_pass=self.mongo_pass,
            mongo_host=self.mongo_host,
            mongo_port=self.mongo_port,
        )

        neo4j_url = f"{self.neo4j_protocol}://{self.neo4j_host}:{self.neo4j_port}"

        self.DB_connections.set_neo4j_utils(
            neo4j_db_name=self.neo4j_db_name,
            neo4j_url=neo4j_url,
            neo4j_user=self.neo4j_user,
            neo4j_password=self.neo4j_password,
        )
