from discord_analyzer.DB_operations.mongodb_interaction import MongoDBOps  # isort: skip
from discord_analyzer.DB_operations.neo4j_utils import Neo4jUtils

from discord_analyzer.DB_operations.network_graph import (  # isort: skip
    make_neo4j_networkx_query_dict,
)


class MongoNeo4jDB:
    def __init__(self, logging, testing=False):
        """
        having both databases in one class

        """
        self.neo4j_utils = None
        self.mongoOps = None
        self.testing = testing
        self.logging = logging

    def set_neo4j_utils(
        self, neo4j_db_name: str, neo4j_url: str, neo4j_user: str, neo4j_password: str
    ):
        """
        store the neo4j utils instance
        """
        self.neo4j_utils = Neo4jUtils()
        self.neo4j_utils.set_neo4j_db_info(
            neo4j_db_name=neo4j_db_name,
            neo4j_url=neo4j_url,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
        )
        self.neo4j_utils.neo4j_database_connect()

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
        self, analytics_data, remove_memberactivities=False, remove_heatmaps=False
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
                    self.neo4j_utils.run_operations_transaction(
                        guildId=guildId,
                        queries_list=queries_list,
                        remove_memberactivities=remove_memberactivities,
                    )
            else:
                self.logging.warning("Testing mode enabled! Not saving any data")
