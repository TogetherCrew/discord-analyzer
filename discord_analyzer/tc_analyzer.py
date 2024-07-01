import logging

from datetime import datetime
from discord_analyzer.metrics.analyzer_memberactivities import MemberActivities
from discord_analyzer.metrics.heatmaps import Heatmaps
from discord_analyzer.metrics.neo4j_analytics import Neo4JAnalytics
from discord_analyzer.metrics.utils.analyzer_db_manager import AnalyzerDBManager
from discord_analyzer.metrics.utils.platform import Platform
from discord_analyzer.schemas import GraphSchema
from discord_analyzer.schemas.platform_configs import DiscordAnalyzerConfig
from discord_analyzer.schemas.platform_configs.config_base import PlatformConfigBase
from discord_analyzer.schemas.platform_configs import DiscordAnalyzerConfig


class TCAnalyzer(AnalyzerDBManager):
    """
    TogetherCrew's Analyzer
    class that handles database connections and data analysis
    """

    def __init__(
        self,
        platform_id: str,
        resources: list[str],
        period: datetime,
        action: dict[str, int],
        window: dict[str, int],
        analyzer_config: PlatformConfigBase = DiscordAnalyzerConfig(),
    ):
        """
        analyze the platform's data
        producing heatmaps, memberactivities, and graph analytics

        Parameters
        -----------
        platform_id : str
            platform to analyze its data
        resources : list[str]
            the resources id for filtering on data
        period : datetime
            the period to compute the analytics for
        action : dict[str, int]
            Parameters for computing different memberactivities
        window : dict[str, int]
            Parameters for the whole analyzer, includes the step size and window size
        analyzer_config : PlatformConfigBase
            the config for analyzer to use
        """
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

        self.platform_id = platform_id
        self.resources = resources
        self.period = period
        self.action = action
        self.window = window
        self.analyzer_config = analyzer_config

        self.platform_utils = Platform(platform_id)
        self.community_id = self.platform_utils.get_community_id()

        self.graph_schema = GraphSchema(platform=analyzer_config.platform)
        self.neo4j_analytics = Neo4JAnalytics(platform_id, self.graph_schema)

        # connect to Neo4j & MongoDB database
        self.database_connect()

    
    def analyze(self, recompute: bool) -> None:
        # TODO: merge run_one and recompute codes
        if recompute:
            self.run_once()
        else:
            self.recompute()

    def run_once(self):
        """Run analysis and append to previous anlaytics"""
        # check if the platform was available
        # if not, will raise an error
        self.check_platform()

        logging.info(f"Creating heatmaps for platform id: {self.platform_id}")

        heatmaps_analysis = Heatmaps(
            platform_id=self.platform_id,
            period=self.period,
            resources=self.resources,
            analyzer_config=self.analyzer_config,
        )
        heatmaps_data = heatmaps_analysis.start(from_start=False)

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            platform_id=self.platform_id,
            graph_schema=self.graph_schema,
            remove_memberactivities=False,
            remove_heatmaps=False,
        )

        memberactivity_analysis = MemberActivities(
            platform_id=self.platform_id,
            resources=self.resources,
            action_config=self.action,
            window_config=self.window,
            analyzer_config=self.analyzer_config,
            analyzer_period=self.period,
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
            platform_id=self.platform_id,
            graph_schema=self.graph_schema,
            remove_heatmaps=False,
            remove_memberactivities=False,
        )

        self.neo4j_analytics.compute_metrics(from_start=False)

        self.platform_utils.update_isin_progress()

    def recompute(self):
        """
        recompute the analytics (heatmaps + memberactivities + graph analytics)
        for a new selection of channels
        """
        # check if the platform was available
        # if not, will raise an error
        self.check_platform()

        logging.info(f"Analyzing the Heatmaps data for platform: {self.platform_id}!")
        heatmaps_analysis = Heatmaps(
            platform_id=self.platform_id,
            period=self.period,
            resources=self.resources,
            analyzer_config=self.analyzer_config,
        )
        heatmaps_data = heatmaps_analysis.start(from_start=True)

        # storing heatmaps since memberactivities use them
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)

        self.DB_connections.store_analytics_data(
            analytics_data=analytics_data,
            platform_id=self.platform_id,
            graph_schema=self.graph_schema,
            remove_memberactivities=False,
            remove_heatmaps=True,
        )

        # run the member_activity analyze
        logging.info(
            f"Analyzing the MemberActivities data for platform: {self.platform_id}!"
        )
        memberactivity_analysis = MemberActivities(
            platform_id=self.platform_id,
            resources=self.resources,
            action_config=self.action,
            window_config=self.window,
            analyzer_config=self.analyzer_config,
            analyzer_period=self.period,
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
            platform_id=self.platform_id,
            graph_schema=self.graph_schema,
            remove_memberactivities=True,
            remove_heatmaps=False,
        )

        self.neo4j_analytics.compute_metrics(from_start=True)
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
