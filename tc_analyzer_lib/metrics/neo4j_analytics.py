# A wrapper to compute the neo4j metrics in cron-job
import logging

from tc_analyzer_lib.algorithms.neo4j_analysis.analyzer_node_stats import NodeStats
from tc_analyzer_lib.algorithms.neo4j_analysis.centrality import Centerality
from tc_analyzer_lib.algorithms.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)
from tc_analyzer_lib.algorithms.neo4j_analysis.louvain import Louvain
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Neo4JAnalytics:
    def __init__(self, platform_id: str, graph_schema: GraphSchema) -> None:
        """
        neo4j metrics to be compute

        Parameters
        ------------
        platform_id : str
            the platform to compute analytics for
        graph_schema : GraphSchema
            the graph schema representative of node and relationship labels
        """
        self.neo4j_ops = Neo4jOps.get_instance()
        self.platform_id = platform_id
        self.log_prefix = f"PLATFORMID: {platform_id} "
        self.graph_schema = graph_schema

    def compute_metrics(self, from_start: bool) -> None:
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
        # we don't need this, as the data will be replaced after
        # if from_start:
        #     self._remove_analytics_interacted_in(guildId)

        self.compute_louvain_algorithm(from_start)
        self.compute_local_clustering_coefficient(from_start)
        self.compute_network_decentrality(from_start)
        self.compute_node_stats(from_start)

    def compute_local_clustering_coefficient(self, from_start: bool):
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
        try:
            # Local Clustering Coefficient
            logging.info(f"{self.log_prefix}Computing LocalClusteringCoefficient")
            lcc = LocalClusteringCoeff(self.platform_id, self.graph_schema)
            lcc.compute(from_start=from_start)
        except Exception as exp:
            logging.error(
                f"{self.log_prefix}Exception in computing LocalClusteringCoefficient, {exp}"
            )

    def compute_fragmentation_score(
        self,
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
        past_window_date : float
            the timestamp for window date
        scale_fragmentation_score : int
            scaling the fragmentation score by a value
            default is `1` meaning no scale
        """
        logging.info(f"{self.log_prefix}Averaging LocalClusteringCoefficient")

        query = f"""
            MATCH ()-[r:{self.graph_schema.interacted_in_rel}]->(g:{self.graph_schema.platform_label} {{id: $platform_id }})
            WHERE r.date >= $past_date
            WITH r.date as date, r.localClusteringCoefficient as lcc
            RETURN
                avg(lcc) * $scale AS fragmentation_score,
                date
        """
        records, _, _ = self.neo4j_ops.neo4j_driver.execute_query(
            query,
            platform_id=self.platform_id,
            scale=scale_fragmentation_score,
            past_date=past_window_date,
        )

        return records

    def compute_network_decentrality(self, from_start: bool):
        """
        compute network decentrality and save results back to neo4j
        """
        try:
            centrality = Centerality(self.platform_id, self.graph_schema)
            # degree decentrality
            _ = centrality.compute_network_decentrality(from_start=from_start)
        except Exception as exp:
            logging.error(
                f"{self.log_prefix}Exception occured in computing Network decentrality, {exp}!"
            )

    def compute_node_stats(self, from_start: bool):
        """
        compute node stats
        each DiscordAccount node could be either
        - "0": meaning Sender
        - "1": Receiver
        - "2": Balanced
        """
        try:
            logging.info(f"{self.log_prefix} computing node stats")
            node_stats = NodeStats(
                platform_id=self.platform_id,
                graph_schema=self.graph_schema,
                threshold=2,
            )
            node_stats.compute_stats(from_start)
        except Exception as exp:
            logging.error(
                f"{self.log_prefix}Exception occured in node stats computation, {exp}"
            )

    def _remove_analytics_interacted_in(self) -> None:
        """
        Remove the INTERACTED_IN relations
        Note: we saved those under the INTERACTED_IN relation

        Parameters:
        --------------
        guildId : str
            the guild we want to delete the relations for
        """
        with self.neo4j_ops.neo4j_driver.session() as session:
            query = f"""
                MATCH (:{self.graph_schema.user_label}) -[
                    r:{self.graph_schema.interacted_in_rel}
                ]->(:{self.graph_schema.platform_label} {{id: $platform_id}})
                DELETE r
            """
            session.run(query=query, platform_id=self.platform_id)

    def compute_louvain_algorithm(self, from_start: bool) -> None:
        """
        compute the louvain algorithm and save the results within the db

        Parameters
        ------------
        guild_id : str
            the guild string that the algorithm would be computed on
        from_start : bool
            compute from the start of the data available or continue the previous
        """
        louvain = Louvain(self.platform_id, self.graph_schema)
        louvain.compute(from_start)
