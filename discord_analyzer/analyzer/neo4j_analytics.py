# A wrapper to compute the neo4j metrics in cron-job
import logging
from datetime import datetime

from discord_analyzer.analysis.neo4j_analysis.analyzer_node_stats import NodeStats
from discord_analyzer.analysis.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)
from discord_analyzer.DB_operations.neo4j_utils import Neo4jUtils

from discord_analyzer.analysis.neo4j_analysis.centrality import (  # isort: skip
    Centerality,
)


class Neo4JAnalytics:
    def __init__(self, neo4j_utils: Neo4jUtils) -> None:
        """
        neo4j metrics to be compute
        input variables are all the neo4j credentials
        """
        self.neo4j_utils = neo4j_utils

    def compute_metrics(self, guildId: str, from_start: bool) -> None:
        """
        compute the essential metrics we wanted for neo4j

        Parameters:
        ------------
        guildId : str
            the specific guild we want to compute metrics for
        neo4j_creds : dict[str, any]
            a dictionary of credentials for neo4j
        from_start : bool
            compute metrics from start or not
            Note: only some metrics support this
            others would be computed from_start=True
        scale_fragmentation_score : int
            scale the fragmentation score
            default is 1 meaning no scaling
        """
        if from_start:
            self._remove_analytics_interacted_in(guildId)

        self.compute_local_clustering_coefficient(guildId, from_start)
        self.compute_network_decentrality(guildId, from_start)
        self.compute_node_stats(guildId, from_start)

    def compute_local_clustering_coefficient(
        self,
        guildId: str,
        from_start: bool,
    ):
        """
        compute localClusteringCoefficient

        Parameters:
        ------------
        guildId : str
            the specific guild we want to compute metrics for
        neo4j_creds : dict[str, any]
            a dictionary of credentials for neo4j
        scale_fragmentation_score : int
            scale the fragmentation score
        from_start : bool
            compute metrics from start or not
            Note: only some metrics support this
            others would be computed from_start=True
        """
        msg = f"GUILDID: {guildId}:"
        try:
            # Local Clustering Coefficient
            logging.info(f"{msg} Computing LocalClusteringCoefficient")
            lcc = LocalClusteringCoeff(gds=self.neo4j_utils.gds)
            lcc.compute(guildId=guildId, from_start=from_start)
        except Exception as exp:
            logging.error(f"{msg} Exception in computing LocalClusteringCoefficient!")
            logging.error(f"Exception: {exp}")

    def compute_fragmentation_score(
        self,
        guildId: str,
        past_window_date: datetime.timestamp,
        scale_fragmentation_score: int = 1,
    ):
        """
        average throught localClusteringCoefficients and group by date
        this is the fragmentation score over each date period

        Note: We can compute this metric in backend,
        so we might not add it to pipeline.
        """
        msg = f"GUILDID: {guildId}:"
        logging.info(f"{msg} Averaging LocalClusteringCoefficient")
        query = """
            MATCH ()-[r:INTERACTED_IN]->(g:Guild {guildId: $guildId })
            WHERE r.date >= $past_date
            WITH r.date as date, r.localClusteringCoefficient as lcc
            RETURN
                avg(lcc) * $scale AS fragmentation_score,
                date
        """
        records, _, _ = self.neo4j_utils.neo4j_driver.execute_query(
            query,
            guildId=guildId,
            scale=scale_fragmentation_score,
            past_date=past_window_date,
        )

        return records

    def compute_network_decentrality(self, guildId: str, from_start: bool):
        """
        compute network decentrality and save results back to neo4j
        """
        try:
            centrality = Centerality(self.neo4j_utils)
            # degree decentrality
            _ = centrality.compute_network_decentrality(
                guildId=guildId, from_start=from_start
            )
        except Exception as exp:
            logging.error("Exception occured in computing Network decentrality!")
            logging.error(f"Exception: {exp}")

    def compute_node_stats(self, guildId: str, from_start: bool):
        """
        compute node stats
        each DiscordAccount node could be either
        - "0": meaning Sender
        - "1": Receiver
        - "2": Balanced
        """
        try:
            logging.info(f"GUILDID: {guildId}: computing node stats")
            node_stats = NodeStats(self.neo4j_utils, threshold=2)
            node_stats.compute_stats(guildId, from_start)
        except Exception as exp:
            logging.error("Exception occured in node stats computation!")
            logging.error(f"Exception: {exp}")

    def _remove_analytics_interacted_in(self, guildId: str) -> None:
        """
        Remove the INTERACTED_IN relations
        Note: we saved those under the INTERACTED_IN relation

        Parameters:
        --------------
        guildId : str
            the guild we want to delete the relations for
        """
        with self.neo4j_utils.neo4j_driver.session() as session:
            query = """
                MATCH (:DiscordAccount) -[r:INTERACTED_IN]->(:Guild {guildId: $guildId})
                DELETE r
            """
            session.run(query=query, guildId=guildId)
