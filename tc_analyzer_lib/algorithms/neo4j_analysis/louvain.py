import logging
from uuid import uuid1

from tc_analyzer_lib.algorithms.neo4j_analysis.utils import ProjectionUtils
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Louvain:
    def __init__(self, platform_id: str, graph_schema: GraphSchema) -> None:
        """
        louvain algorithm wrapper to compute
        """
        self.neo4j_ops = Neo4jOps.get_instance()
        self.platform_id = platform_id
        self.graph_schema = graph_schema

        self.projection_utils = ProjectionUtils(
            platform_id=platform_id, graph_schema=graph_schema
        )
        self.log_prefix = f"PLATFORMID: {platform_id} "

    def compute(self, from_start: bool = False) -> None:
        """
        compute the louvain modularity score for a guild

        Parameters
        ------------
        from_start : bool
            whether to compute the metric from the first day or not
            if True, then would compute from start
            default is False
        """

        computable_dates = self.projection_utils.get_dates()

        # compute for each date
        to_compute: set[float]
        if from_start:
            to_compute = computable_dates
        else:
            computed_dates = self.get_computed_dates()
            to_compute = computable_dates - computed_dates

        for date in to_compute:
            try:
                self.louvain_computation_wrapper(date)
            except Exception as exp:
                logging.error(
                    f"Exception: {self.log_prefix}Louvain Modularity "
                    f" computation for date: {date}, exp: {exp}"
                )

    def louvain_computation_wrapper(self, date: float) -> None:
        """
        a wrapper for louvain modularity computation process
        we're doing the projection here and computing on that,
        then we'll drop the pojection

        Parameters:
        ------------
        date : float
            timestamp of the relation
        """
        graph_projected_name = f"GraphLouvain_{uuid1()}"
        self.projection_utils.project_temp_graph(
            graph_name=graph_projected_name,
            weighted=True,
            date=date,
            relation_direction="NATURAL",
        )

        # get the results as pandas dataframe
        self.compute_graph_louvain(date=date, graph_name=graph_projected_name)

        # dropping the computed date
        _ = self.neo4j_ops.gds.run_cypher(
            """
            CALL gds.graph.drop($graph_projected_name)
            """,
            {
                "graph_projected_name": graph_projected_name,
            },
        )

    def get_computed_dates(self) -> set[float]:
        """
        get localClusteringCoeff computed dates

        Returns:
        ----------
        computed_dates : set[float]
            the computation dates
        """
        # getting the dates computed before
        query = f"""
            MATCH (g:{self.graph_schema.platform_label} {{id: $platform_id}})
                -[r:HAVE_METRICS]->(g)
            WHERE r.louvainModularityScore IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = self.projection_utils.get_computed_dates(
            query, platform_id=self.platform_id
        )

        return computed_dates

    def compute_graph_louvain(self, date: float, graph_name: str) -> None:
        """
        compute louvain algorithm for the projected graph and
        save the results back into db

        Parameters:
        ------------
        date : float
            timestamp of the relation
        graph_name : str
            the operation would be done on the graph
        """
        try:
            _ = self.neo4j_ops.gds.run_cypher(
                f"""
                    CALL gds.louvain.stats($graph_name)
                    YIELD modularity
                    WITH modularity
                    MATCH (g:{self.graph_schema.platform_label} {{id: $platform_id}})
                    MERGE (g) -[r:HAVE_METRICS {{
                        date: $date
                    }}]-> (g)
                    SET r.louvainModularityScore = modularity
                    """,
                {
                    "graph_name": graph_name,
                    "platform_id": self.platform_id,
                    "date": date,
                },
            )
        except Exception as exp:
            logging.error(
                f"{self.log_prefix} Error in computing "
                f"louvain modularity algorithm, {exp}"
            )
