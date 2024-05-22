import logging

from discord_analyzer.analyzer.analyzer_heatmaps import Heatmaps
from discord_analyzer.analyzer.analyzer_memberactivities import MemberActivities
from discord_analyzer.analyzer.neo4j_analytics import Neo4JAnalytics
from discord_analyzer.analyzer.utils.analyzer_db_manager import AnalyzerDBManager
from discord_analyzer.analyzer.utils.guild import Guild


class RnDaoAnalyzer(AnalyzerDBManager):
    """
    RnDaoAnalyzer
    class that handles database connections and data analysis
    """

    def __init__(self, guild_id: str, testing=False):
        """
        Class initiation function
        """
        """ Testing, prevents from data upload"""
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

        self.testing = testing
        self.guild_object = Guild(guild_id)
        self.guild_id = guild_id
        self.community_id = self.guild_object.get_community_id()

    def setup_neo4j_metrics(self) -> None:
        """
        setup the neo4j analytics wrapper
        """

        self.neo4j_analytics = Neo4JAnalytics()

    def run_once(self):
        """Run analysis once (Wrapper)"""
        # check if the guild was available
        # if not, will raise an error
        self.check_guild()

        logging.info(f"Creating heatmaps for guild: {self.guild_id}")

        heatmaps_analysis = Heatmaps(self.DB_connections, self.testing)
        heatmaps_data = heatmaps_analysis.analysis_heatmap(self.guild_id)

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.guild_id,
            community_id=self.community_id,
            remove_memberactivities=False,
            remove_heatmaps=False,
        )

        memberactivities_analysis = MemberActivities(self.DB_connections)
        (
            member_activities_data,
            member_acitivities_networkx_data,
        ) = memberactivities_analysis.analysis_member_activity(
            self.guild_id, self.connection_str
        )

        analytics_data = {}
        # storing whole data into a dictinoary
        analytics_data["heatmaps"] = None
        analytics_data["memberactivities"] = (
            member_activities_data,
            member_acitivities_networkx_data,
        )

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.guild_id,
            community_id=self.community_id,
            remove_heatmaps=False,
            remove_memberactivities=False,
        )

        self.neo4j_analytics.compute_metrics(guildId=self.guild_id, from_start=False)

        self.guild_object.update_isin_progress()

    def recompute_analytics(self):
        """
        recompute the memberactivities (and heatmaps in case needed)
         for a new selection of channels


        - first it would update the channel selection in Core.Platform

        - Second the memebracitivites collection
         of the input guildId would become empty

        - Third we would have the analytics running again on the
         new channel selection (analytics would be inserted in memebractivities)


        Returns:
        ---------
        `None`
        """
        # check if the guild was available
        # if not, will raise an error
        self.check_guild()

        heatmaps_analysis = Heatmaps(self.DB_connections, self.testing)

        logging.info(f"Analyzing the Heatmaps data for guild: {self.guild_id}!")
        heatmaps_data = heatmaps_analysis.analysis_heatmap(
            guildId=self.guild_id, from_start=True
        )

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.guild_id,
            community_id=self.community_id,
            remove_memberactivities=False,
            remove_heatmaps=True,
        )

        # run the member_activity analyze
        logging.info(f"Analyzing the MemberActivities data for guild: {self.guild_id}!")
        memberactivity_analysis = MemberActivities(self.DB_connections)
        (
            member_activities_data,
            member_acitivities_networkx_data,
        ) = memberactivity_analysis.analysis_member_activity(
            self.guild_id, self.connection_str, from_start=True
        )

        # storing whole data into a dictinoary
        analytics_data = {}
        # storing whole data into a dictinoary
        analytics_data["heatmaps"] = None
        analytics_data["memberactivities"] = (
            member_activities_data,
            member_acitivities_networkx_data,
        )

        logging.info(f"Storing analytics data for guild: {self.guild_id}!")
        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.guild_id,
            community_id=self.community_id,
            remove_memberactivities=True,
            remove_heatmaps=False,
        )

        self.neo4j_analytics.compute_metrics(guildId=self.guild_id, from_start=True)
        self.guild_object.update_isin_progress()

    def check_guild(self):
        """
        check if the guild is available
        """
        exist = self.guild_object.check_existance()
        if exist is False:
            raise ValueError(f"Guild with guildId: {self.guild_id} doesn't exist!")
