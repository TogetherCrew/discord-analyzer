import logging

from graphdatascience import GraphDataScience
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError, DatabaseError, TransientError


class Neo4jUtils:
    def __init__(self) -> None:
        """
        neo4j utility functions

        Parameters:
        ------------
        logger : python logger
        the instance to log everything, default is None
        """
        # Neo4J credentials
        self.neo4j_dbName = ""
        self.neo4j_url = ""
        self.neo4j_auth = (None, None)
        self.neo4j_driver = None
        self.guild_msg = ""

    def set_neo4j_db_info(self, neo4j_db_name, neo4j_url, neo4j_user, neo4j_password):
        """
        Neo4j Database information setter

        Parameters:
        -------------
        neo4j_db_ame : str
            the database name to save the results in it
        neo4j_url : str
            the string of neo4j url
        neo4j_user : str
            neo4j username to connect
        neo4j_password : str
            neo4j database password
        """
        neo4j_auth = (neo4j_user, neo4j_password)

        self.neo4j_url = neo4j_url
        self.neo4j_auth = neo4j_auth
        self.neo4j_dbName = neo4j_db_name

    def neo4j_database_connect(self):
        """
        connect to neo4j database and set the database driver it the class
        """
        with GraphDatabase.driver(self.neo4j_url, auth=self.neo4j_auth) as driver:
            driver.verify_connectivity()

        self.neo4j_driver = driver
        self.gds = self.setup_gds()

    def setup_gds(self):
        gds = GraphDataScience(self.neo4j_url, self.neo4j_auth)

        return gds

    def _run_query(self, tx, query):
        """
        handle neo4j queries transaction
        """
        try:
            tx.run(query)
        except TransientError as err:
            logging.error("Neo4j transient error!")
            logging.error(f"Code: {err.code}, message: {err.message}")
        except DatabaseError as err:
            logging.error("Neo4J database error")
            logging.error(f"Code: {err.code}, message: {err.message}")
        except ClientError as err:
            logging.error("Neo4j Client Error!")
            logging.error(f"Code: {err.code}, message: {err.message}")

    def store_data_neo4j(self, query_list: list[str], message: str = ""):
        """
        store data into neo4j using the given query list

        Parameters:
        ------------
        query_list : list of str
            list of strings to add data into neo4j
            min length is 1
        message : str
            the message to be printed out
            default is nothing
        """
        try:
            # session = await self.neo4j_driver.session(database=self.neo4j_dbName)
            with self.neo4j_driver.session(database=self.neo4j_dbName) as session:
                query_count = len(query_list)
                for idx, query in enumerate(query_list):
                    logging.info(
                        f"{message} NEO4J Transaction: Batch {idx + 1}/{query_count}"
                    )
                    session.execute_write(self._run_query, query=query)
        except Exception as e:
            logging.error(f"Couldn't execute  Neo4J DB transaction, exception: {e}")

    def empty_neo4j_interacted_relationship(self, guildId):
        """
        remove the interactions between `DiscordAccount`s
          that are connected to a guild

        Parameters:
        ------------
        guildId : str
            the guild id that the users are connected to it

        """
        delete_relationship_query = self._create_guild_rel_deletion_query(
            guildId=guildId
        )

        self.store_data_neo4j(
            [delete_relationship_query], message=f"GUILDID: {guildId}:"
        )

    def run_operations_transaction(
        self, guildId, queries_list, remove_memberactivities
    ):
        """
        do the deletion and insertion operations inside a transaction

        Parameters:
        ------------
        guildId : str
            the guild id that the users are connected to it
            which we're going to delete the relations of it
        queries_list : list
            list of strings to add data into neo4j
            min length is 1
        remove_memberactivities : bool
            if True, remove the old data specified in that guild
        """
        self.guild_msg = f"GUILDID: {guildId}:"

        transaction_queries = []
        if remove_memberactivities:
            logging.info(
                f"{self.guild_msg} Neo4J GuildId accounts relation will be removed!"
            )
            delete_relationship_query = self._create_guild_rel_deletion_query(
                guildId=guildId
            )
            transaction_queries.append(delete_relationship_query)

        # logging.info(queries_list)
        transaction_queries.extend(queries_list)

        self.store_data_neo4j(transaction_queries, message=self.guild_msg)

    def _create_guild_rel_deletion_query(
        self, guildId: str, relation_name: str = "INTERACTED_WITH"
    ):
        """
        create a query to delete the relationships
        between DiscordAccount users in a specific guild

        Parameters:
        -------------
        guildId : str
            the guild id that the users are connected to it
        relation_name : str
            the relation we want to delete

        Returns:
        ------------
        final_query : str
            the final query to remove the relationships
        """

        delete_relationship_query = f"""
          MATCH 
            (:DiscordAccount) 
                -[r:{relation_name} {{guildId: '{guildId}'}}]-(:DiscordAccount)
            DETACH DELETE r"""

        return delete_relationship_query
