# A wrapper to compute the neo4j metrics in cron-job
import logging

from discord_analyzer.analysis.neo4j_analysis.analyzer_node_stats import NodeStats
from discord_analyzer.analysis.neo4j_analysis.centrality import Centerality
from discord_analyzer.analysis.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Neo4JAnalytics:
    def __init__(self, neo4j_ops: Neo4jOps) -> None:
        """
        neo4j metrics to be compute
        input variables are all the neo4j credentials
        """
        self.neo4j_ops = neo4j_ops

    def compute_metrics(self, guildId: str, from_start: bool) -> None:
        """
        compute the essential metrics we wanted for neo4j

        Parameters:
        ------------
        guildId : str
            the specific guild we want to compute metrics for
        from_start : bool
            compute metrics from start or not
            Note: only some metrics support this
            others would be computed from_start=True
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
        from_start : bool
            compute metrics from start or not
            Note: only some metrics support this
            others would be computed from_start=True
        """
        msg = f"GUILDID: {guildId}:"
        try:
            # Local Clustering Coefficient
            logging.info(f"{msg} Computing LocalClusteringCoefficient")
            lcc = LocalClusteringCoeff(gds=self.neo4j_ops.gds)
            lcc.compute(guildId=guildId, from_start=from_start)
        except Exception as exp:
            logging.error(
                f"{msg} Exception in computing LocalClusteringCoefficient, {exp}"
            )

    def compute_fragmentation_score(
        self,
        guildId: str,
        past_window_date: float,
        scale_fragmentation_score: int = 1,
    ):
        """
        average throught localClusteringCoefficients and group by date
        this is the fragmentation score over each date period

        Note: We can compute this metric in backend,
        so we might not add it to pipeline.

        Parameters:
        --------------
        guildId : str
            the guildId to use
        past_window_date : float
            the timestamp for window date
        scale_fragmentation_score : int
            scaling the fragmentation score by a value
            default is `1` meaning no scale
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
        records, _, _ = self.neo4j_ops.neo4j_driver.execute_query(
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
        msg = f"GUILDID: {guildId}:"
        try:
            centrality = Centerality(self.neo4j_ops)
            # degree decentrality
            _ = centrality.compute_network_decentrality(
                guildId=guildId, from_start=from_start
            )
        except Exception as exp:
            logging.error(
                f"{msg} Exception occured in computing Network decentrality, {exp}!"
            )

    def compute_node_stats(self, guildId: str, from_start: bool):
        """
        compute node stats
        each DiscordAccount node could be either
        - "0": meaning Sender
        - "1": Receiver
        - "2": Balanced
        """
        msg = f"GUILDID: {guildId}:"
        try:
            logging.info(f"{msg}: computing node stats")
            node_stats = NodeStats(self.neo4j_ops, threshold=2)
            node_stats.compute_stats(guildId, from_start)
        except Exception as exp:
            logging.error(f"{msg} Exception occured in node stats computation, {exp}")

    def _remove_analytics_interacted_in(self, guildId: str) -> None:
        """
        Remove the INTERACTED_IN relations
        Note: we saved those under the INTERACTED_IN relation

        Parameters:
        --------------
        guildId : str
            the guild we want to delete the relations for
        """
        with self.neo4j_ops.neo4j_driver.session() as session:
            query = """
                MATCH (:DiscordAccount) -[r:INTERACTED_IN]->(:Guild {guildId: $guildId})
                DELETE r
            """
            session.run(query=query, guildId=guildId)
