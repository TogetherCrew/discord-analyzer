#!/usr/bin/env python3
import logging
import os
import sys

from discord_analyzer.analyzer.analyzer_heatmaps import Heatmaps
from discord_analyzer.analyzer.analyzer_memberactivities import Member_activities
from discord_analyzer.analyzer.base_analyzer import Base_analyzer
from discord_analyzer.analyzer.neo4j_analytics import Neo4JAnalytics
from discord_analyzer.models.GuildsRnDaoModel import GuildsRnDaoModel
from discord_analyzer.models.HeatMapModel import HeatMapModel
from discord_analyzer.models.RawInfoModel import RawInfoModel
from dotenv import load_dotenv


class RnDaoAnalyzer(Base_analyzer):
    """
    RnDaoAnalyzer
    class that handles database connection and data analysis
    """

    def __init__(self, testing=False):
        """
        Class initiation function
        """
        """ Testing, prevents from data upload"""
        self.testing = testing
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

    def setup_neo4j_metrics(self) -> None:
        """
        setup the neo4j analytics wrapper
        """

        self.neo4j_analytics = Neo4JAnalytics(neo4j_ops=self.DB_connections.neo4j_ops)

    def run_once(self, guildId):
        """Run analysis once (Wrapper)"""

        guilds_c = GuildsRnDaoModel(
            self.DB_connections.mongoOps.mongo_db_access.db_mongo_client["RnDAO"]
        )

        guilds = guilds_c.get_connected_guilds(guildId)

        logging.info(f"Creating heatmaps for {guilds}")

        # each guild data in a nested dictionary format
        guilds_data = {}

        for guild in guilds:
            logging.info(f"Doing Analytics for {guild}")

            heatmaps_analysis = Heatmaps(self.DB_connections, self.testing)
            heatmaps_data = heatmaps_analysis.analysis_heatmap(guild)

            # storing heatmaps since memberactivities use them
            analytics_data = {}
            analytics_data[f"{guild}"] = {
                "heatmaps": heatmaps_data,
                "memberactivities": (
                    None,
                    None,
                ),
            }
            self.DB_connections.store_analytics_data(
                analytics_data=analytics_data,
                remove_memberactivities=False,
                remove_heatmaps=False,
            )

            memberactivities_analysis = Member_activities(
                self.DB_connections, logging=logging
            )
            (
                member_activities_data,
                member_acitivities_networkx_data,
            ) = memberactivities_analysis.analysis_member_activity(
                guild, self.connection_str
            )

            # storing whole data into a dictinoary
            guilds_data[f"{guild}"] = {
                "heatmaps": None,
                "memberactivities": (
                    member_activities_data,
                    member_acitivities_networkx_data,
                ),
            }

            self.DB_connections.store_analytics_data(
                analytics_data=guilds_data,
                remove_heatmaps=False,
                remove_memberactivities=False,
            )

        self.neo4j_analytics.compute_metrics(guildId=guildId, from_start=False)

        self._update_isin_progress(guildId=guild)

    def get_guilds(self):
        """Returns the list of all guilds"""
        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client
        rawinfo_c = RawInfoModel(client)

        logging.info(f"Listed guilds {rawinfo_c.database.list_collection_names()}")

    def recompute_analytics_on_guilds(self, guildId_list):
        """
        recompute the analytics for the guilds available in RnDAO table
        if the guildId_list wasn't available in RnDAO then don't recompute the analytics

        Parameters:
        --------------
        guildId_list : list of str
            list of `guildId`s
            Input can be `None` meaning recompute for all guilds

        Returns:
        ---------
        `None`
        """
        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        # check if the guild was available in RnDAO table
        guilds_c = GuildsRnDaoModel(client["RnDAO"])
        guilds = guilds_c.get_connected_guilds(guildId_list)

        logging.info(f"Recomputing analytics for {guilds}")

        for guildId in guilds:
            self.recompute_analytics(guildId)

            self._update_isin_progress(guildId=guildId)

        return None

    def recompute_analytics(self, guildId):
        """
        recompute the memberactivities (and heatmaps in case needed)
         for a new selection of channels


        - first it would update the channel selection in RnDAO

        - Second the memebracitivites collection
         of the input guildId would become empty

        - Third we would have the analytics running again on the
         new channel selection (analytics would be inserted in memebractivities)


        Parameters:
        -------------
        guildId : str
            the guildId to remove its collection data

        Returns:
        ---------
        `None`
        """

        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        guild_c = GuildsRnDaoModel(client["RnDAO"])
        selectedChannels = guild_c.get_guild_channels(guildId=guildId)

        if selectedChannels != []:
            # get the `channel_id`s
            channel_id_list = list(
                map(lambda channel_info: channel_info["channelId"], selectedChannels)
            )
        else:
            channel_id_list = []

        # check if all the channels were available in heatmaps
        is_available = self.DB_connections.mongoOps.check_heatmaps(
            guildId=guildId,
            selectedChannels=channel_id_list,
            heatmap_model=HeatMapModel,
        )

        # initialize variable
        heatmaps_data = None
        heatmaps_analysis = Heatmaps(self.DB_connections, self.testing)
        heatmap_isempty = heatmaps_analysis.is_empty(guildId)

        # if not available we should remove heatmaps data
        # and run the analytics for heatmaps too
        # TODO: condition update
        is_available = False
        if not is_available or heatmap_isempty:
            logging.info(f"Analyzing the Heatmaps data for guild: {guildId}!")
            heatmaps_data = heatmaps_analysis.analysis_heatmap(
                guildId=guildId, from_start=True
            )

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data[f"{guildId}"] = {
            "heatmaps": heatmaps_data,
            "memberactivities": (
                None,
                None,
            ),
        }
        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            remove_memberactivities=False,
            remove_heatmaps=not is_available,
        )

        # run the member_activity analyze
        logging.info(f"Analyzing the MemberActivities data for guild: {guildId}!")
        memberactivity_analysis = Member_activities(
            self.DB_connections, logging=logging
        )
        (
            member_activities_data,
            member_acitivities_networkx_data,
        ) = memberactivity_analysis.analysis_member_activity(
            guildId, self.connection_str, from_start=True
        )

        # storing whole data into a dictinoary
        analytics_data = {}
        analytics_data[f"{guildId}"] = {
            "heatmaps": None,
            "memberactivities": (
                member_activities_data,
                member_acitivities_networkx_data,
            ),
        }

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            remove_memberactivities=True,
            remove_heatmaps=False,
        )

        self.neo4j_analytics.compute_metrics(guildId=guildId, from_start=True)

        self._update_isin_progress(guildId=guildId)

        # returning a value when the jobs finished
        return True

    def _update_isin_progress(self, guildId):
        """
        update isInProgress field of RnDAO collection

        Parameters:
        ------------
        guildId : str
            the guildId to update its document
        """
        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        client["RnDAO"]["guilds"].update_one(
            {"guildId": guildId}, {"$set": {"isInProgress": False}}
        )


# get guildId from command, if not given return None
# python ./analyzer.py guildId


def getParamsFromCmd():
    """
    get guildId and recompute analysis arguments from cmd
    the second argument should be guildId,
    and the third one should be recompute_analysis
    (if third args not given, then recompute analysis will be False)

    Returns:
    ----------
    guildId : str
        the guildId to analyze
    recompute_analysis : bool
        whether to recompute the analysis or just run once

    """
    args = sys.argv
    guildId = None
    recompute_analysis = False
    if len(args) == 2:
        guildId = args[1]
    elif len(args) == 3:
        guildId = args[1]
        recompute_analysis = True
    return guildId, recompute_analysis


if __name__ == "__main__":
    load_dotenv()

    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.INFO)
    analyzer = RnDaoAnalyzer()

    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASS")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")

    neo4j_creds = {}
    neo4j_creds["db_name"] = os.getenv("NEO4J_DB", "")
    neo4j_creds["protocol"] = os.getenv("NEO4J_PROTOCOL", "")
    neo4j_creds["host"] = os.getenv("NEO4J_HOST", "")
    neo4j_creds["port"] = os.getenv("NEO4J_PORT", "")
    neo4j_creds["password"] = os.getenv("NEO4J_PASSWORD", "")
    neo4j_creds["user"] = os.getenv("NEO4J_USER", "")

    neo4j_user = os.getenv("NEO4J_USER", "")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "")

    analyzer.set_mongo_database_info(
        mongo_db_host=host,
        mongo_db_password=password,
        mongo_db_user=user,
        mongo_db_port=port,
    )

    analyzer.set_neo4j_database_info(neo4j_creds=neo4j_creds)

    guildId, recompute_analysis = getParamsFromCmd()
    analyzer.database_connect()
    analyzer.setup_neo4j_metrics()

    if not recompute_analysis:
        analyzer.run_once(guildId)
    else:
        analyzer.recompute_analytics_on_guilds(guildId)
