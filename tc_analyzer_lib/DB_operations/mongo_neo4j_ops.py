import logging

from tc_analyzer_lib.DB_operations.mongodb_interaction import MongoDBOps
from tc_analyzer_lib.DB_operations.network_graph import NetworkGraph
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps, Query


class MongoNeo4jDB:
    def __init__(self, testing=False):
        """
        having both databases in one class

        """
        self.neo4j_ops = Neo4jOps.get_instance()
        self.mongoOps = None
        self.testing = testing

    def set_mongo_db_ops(self):
        """
        setup the MongoDBOps class with the parameters needed
        """
        self.mongoOps = MongoDBOps()
        self.mongoOps.set_mongo_db_access()
        try:
            info = self.mongoOps.mongo_db_access.db_mongo_client.server_info()
            logging.info(f"MongoDB Connected, server info: {info}")
        except Exception as exp:
            logging.error(f"Error connecting to Mongodb, exp: {exp}")

    def store_analytics_data(
        self,
        analytics_data: dict,
        platform_id: str,
        graph_schema: GraphSchema,
        remove_memberactivities: bool = False,
        remove_heatmaps: bool = False,
    ):
        """
        store the analytics data into database
        all data are in format of nested dictionaries which

        Parameters:
        -------------
        analytics_data : dict
            a nested dictinoary with keys as `heatmaps`, and `memberactivities`
            values of the heatmaps is a list of dictinoaries
            and memberactivities is a tuple of memberactivities dictionary list
            and memebractivities networkx object dictionary list
        platform_id: str
            what the data is related to
        graph_schema : GraphSchema
            the schema for graph to be saved
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
        heatmaps_data = analytics_data["heatmaps"]
        (memberactivities_data, memberactivities_networkx_data) = analytics_data[
            "memberactivities"
        ]

        if not self.testing:
            # mongodb transactions
            self.mongoOps._do_analytics_write_transaction(
                platform_id=platform_id,
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
                network_graph = NetworkGraph(graph_schema, platform_id)
                queries_list = network_graph.make_neo4j_networkx_query_dict(
                    networkx_graphs=memberactivities_networkx_data,
                )
                self.run_operations_transaction(
                    platform_id=platform_id,
                    queries_list=queries_list,
                    remove_memberactivities=remove_memberactivities,
                )
        else:
            logging.warning("Testing mode enabled! Not saving any data")

    def run_operations_transaction(
        self, platform_id: str, queries_list: list[Query], remove_memberactivities: bool
    ) -> None:
        """
        do the deletion and insertion operations inside a transaction

        Parameters:
        ------------
        platform_id : str
            the guild id that the users are connected to it
            which we're going to delete the relations of it
        queries_list : list
            list of strings to add data into neo4j
            min length is 1
        remove_memberactivities : bool
            if True, remove the old data specified in that guild
        """
        self.guild_msg = f"platform_id: {platform_id}:"

        transaction_queries: list[Query] = []
        if remove_memberactivities:
            logging.info(
                f"{self.guild_msg} Neo4J platform_id accounts relation will be removed!"
            )
            delete_relationship_query = self._create_guild_rel_deletion_query(
                platform_id=platform_id
            )
            transaction_queries.append(delete_relationship_query)

        transaction_queries.extend(queries_list)

        self.neo4j_ops.run_queries_in_batch(transaction_queries, message=self.guild_msg)

    def _create_guild_rel_deletion_query(
        self, platform_id: str, relation_name: str = "INTERACTED_WITH"
    ) -> Query:
        """
        create a query to delete the relationships
        between DiscordAccount users in a specific guild

        Parameters:
        -------------
        platform_id : str
            the guild id that the users are connected to it
        relation_name : str
            the relation we want to delete

        Returns:
        ------------
        final_query : str
            the final query to remove the relationships
        """
        query_str = f"""
          MATCH
            (:DiscordAccount)
                -[r:{relation_name} {{platformId: '{platform_id}'}}]-(:DiscordAccount)
            DETACH DELETE r"""

        parameters = {
            "relation_name": relation_name,
            "platform_id": platform_id,
        }

        query = Query(
            query=query_str,
            parameters=parameters,
        )
        return query
