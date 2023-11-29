import logging

from tc_neo4j_lib.neo4j_ops import Neo4jOps

from discord_analyzer.DB_operations.mongodb_interaction import MongoDBOps
from discord_analyzer.DB_operations.network_graph import make_neo4j_networkx_query_dict


class MongoNeo4jDB:
    def __init__(self, testing=False):
        """
        having both databases in one class

        """
        self.neo4j_ops = None
        self.mongoOps = None
        self.testing = testing

    def set_neo4j_utils(
        self,
        db_name: str,
        host: str,
        port: str,
        protocol: str,
        user: str,
        password: str,
    ):
        """
        store the neo4j utils instance
        """
        self.neo4j_ops = Neo4jOps()
        self.neo4j_ops.set_neo4j_db_info(
            neo4j_db_name=db_name,
            neo4j_protocol=protocol,
            neo4j_user=user,
            neo4j_password=password,
            neo4j_host=host,
            neo4j_port=port,
        )
        self.neo4j_ops.neo4j_database_connect()

    def set_mongo_db_ops(
        self, mongo_user: str, mongo_pass: str, mongo_host: str, mongo_port: str
    ):
        """
        setup the MongoDBOps class with the parameters needed

        """
        self.mongoOps = MongoDBOps(
            user=mongo_user, password=mongo_pass, host=mongo_host, port=mongo_port
        )
        self.mongoOps.set_mongo_db_access()

    def store_analytics_data(
        self,
        analytics_data: dict,
        remove_memberactivities: bool = False,
        remove_heatmaps: bool = False,
    ):
        """
        store the analytics data into database
        all data are in format of nested dictionaries which

        Parameters:
        -------------
        analytics_data : dictionary
            a nested dictinoary with keys as guildId
            and values as heatmaps and memberactivities data
            heatmaps is also a list of dictinoaries
            and memberactivities is a tuple of memberactivities dictionary list
             and memebractivities networkx object dictionary list
        remove_memberactivities : bool
            remove the whole memberactivity data and insert
            default is `False` which means don't delete the existing data
        remove_heatmaps : bool
            remove the whole heatmap data and insert
            default is `False` which means don't delete the existing data

        Returns:
        ----------
        `None`
        """
        for guildId in analytics_data.keys():
            heatmaps_data = analytics_data[guildId]["heatmaps"]
            (memberactivities_data, memberactivities_networkx_data) = analytics_data[
                guildId
            ]["memberactivities"]

            if not self.testing:
                # mongodb transactions
                self.mongoOps._do_analytics_write_transaction(
                    guildId=guildId,
                    delete_heatmaps=remove_heatmaps,
                    delete_member_acitivities=remove_memberactivities,
                    acitivties_list=memberactivities_data,
                    heatmaps_list=heatmaps_data,
                )

                # neo4j transactions
                if (
                    memberactivities_networkx_data is not None
                    and memberactivities_networkx_data != []
                ):
                    queries_list = make_neo4j_networkx_query_dict(
                        networkx_graphs=memberactivities_networkx_data, guildId=guildId
                    )
                    self.run_operations_transaction(
                        guildId=guildId,
                        queries_list=queries_list,
                        remove_memberactivities=remove_memberactivities,
                    )
            else:
                logging.warning("Testing mode enabled! Not saving any data")

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

        self.neo4j_ops.store_data_neo4j(transaction_queries, message=self.guild_msg)

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
