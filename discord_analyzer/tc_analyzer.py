import logging

from discord_analyzer.metrics.analyzer_memberactivities import MemberActivities
from discord_analyzer.metrics.heatmaps import Heatmaps
from discord_analyzer.metrics.neo4j_analytics import Neo4JAnalytics
from discord_analyzer.metrics.utils.analyzer_db_manager import AnalyzerDBManager
from discord_analyzer.metrics.utils.platform import Platform
from discord_analyzer.schemas.platform_configs import DiscordAnalyzerConfig


class TCAnalyzer(AnalyzerDBManager):
    """
    TCAnalyzer
    class that handles database connections and data analysis
    """

    def __init__(
        self,
        platform_id: str,
    ):
        """
        Class initiation function
        """
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

        # hard-coded for now
        # TODO: define a structure and make it read from db
        self.analyzer_config = DiscordAnalyzerConfig()

        self.neo4j_analytics = Neo4JAnalytics()
        self.platform_utils = Platform(platform_id)
        self.platform_id = platform_id
        self.community_id = self.platform_utils.get_community_id()

    def run_once(self):
        """Run analysis once (Wrapper)"""
        # check if the platform was available
        # if not, will raise an error
        self.check_platform()

        logging.info(f"Creating heatmaps for platform id: {self.platform_id}")

        heatmaps_analysis = Heatmaps(
            platform_id=self.platform_id,
            period=self.platform_utils.get_platform_period(),
            resources=self.platform_utils.get_platform_resources(),
            analyzer_config=self.analyzer_config,
        )
        heatmaps_data = heatmaps_analysis.start(from_start=False)

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.platform_id,
            community_id=self.community_id,
            remove_memberactivities=False,
            remove_heatmaps=False,
        )

        window, action = self.platform_utils.get_platform_analyzer_params()
        memberactivity_analysis = MemberActivities(
            platform_id=self.platform_id,
            resources=self.platform_utils.get_platform_resources(),
            action_config=action,
            window_config=window,
            analyzer_config=self.analyzer_config,
            analyzer_period=self.platform_utils.get_platform_period(),
        )
        (
            member_activities_data,
            member_acitivities_networkx_data,
        ) = memberactivity_analysis.analysis_member_activity(from_start=False)

        analytics_data = {}
        # storing whole data into a dictinoary
        analytics_data["heatmaps"] = None
        analytics_data["memberactivities"] = (
            member_activities_data,
            member_acitivities_networkx_data,
        )

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.platform_id,
            community_id=self.community_id,
            remove_heatmaps=False,
            remove_memberactivities=False,
        )

        self.neo4j_analytics.compute_metrics(guildId=self.platform_id, from_start=False)

        self.platform_utils.update_isin_progress()

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
        # check if the platform was available
        # if not, will raise an error
        self.check_platform()

        logging.info(f"Analyzing the Heatmaps data for platform: {self.platform_id}!")
        heatmaps_analysis = Heatmaps(
            platform_id=self.platform_id,
            period=self.platform_utils.get_platform_period(),
            resources=self.platform_utils.get_platform_resources(),
            analyzer_config=self.analyzer_config,
        )
        heatmaps_data = heatmaps_analysis.start(from_start=True)

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.platform_id,
            community_id=self.community_id,
            remove_memberactivities=False,
            remove_heatmaps=True,
        )

        # run the member_activity analyze
        logging.info(
            f"Analyzing the MemberActivities data for platform: {self.platform_id}!"
        )
        window, action = self.platform_utils.get_platform_analyzer_params()
        memberactivity_analysis = MemberActivities(
            platform_id=self.platform_id,
            resources=self.platform_utils.get_platform_resources(),
            action_config=action,
            window_config=window,
            analyzer_config=self.analyzer_config,
            analyzer_period=self.platform_utils.get_platform_period(),
        )
        (
            member_activities_data,
            member_acitivities_networkx_data,
        ) = memberactivity_analysis.analysis_member_activity(from_start=True)

        # storing whole data into a dictinoary
        analytics_data = {}
        # storing whole data into a dictinoary
        analytics_data["heatmaps"] = None
        analytics_data["memberactivities"] = (
            member_activities_data,
            member_acitivities_networkx_data,
        )

        logging.info(f"Storing analytics data for platform: {self.platform_id}!")
        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            guild_id=self.platform_id,
            community_id=self.community_id,
            remove_memberactivities=True,
            remove_heatmaps=False,
        )

        self.neo4j_analytics.compute_metrics(guildId=self.platform_id, from_start=True)
        self.platform_utils.update_isin_progress()

    def check_platform(self):
        """
        check if the platform is available
        """
        exist = self.platform_utils.check_existance()
        if not exist:
            raise ValueError(
                f"Platform with platform_id: {self.platform_id} doesn't exist!"
            )
