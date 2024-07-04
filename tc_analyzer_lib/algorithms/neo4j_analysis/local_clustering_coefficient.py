import logging
from uuid import uuid1

from tc_analyzer_lib.algorithms.neo4j_analysis.utils import ProjectionUtils
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib import Neo4jOps


class LocalClusteringCoeff:
    def __init__(self, platform_id: str, graph_schema: GraphSchema) -> None:
        self.gds = Neo4jOps.get_instance().gds
        self.graph_schema = graph_schema
        self.platform_id = platform_id

        self.projection_utils = ProjectionUtils(
            platform_id=self.platform_id, graph_schema=self.graph_schema
        )
        self.log_prefix = f"PLATFORMID: {platform_id} "

    def compute(self, from_start: bool = False) -> None:
        """
        computing the localClusteringCoefficient
        per date of each interaction and saving them in nodes


        Parameters:
        ------------
        from_start : bool
            whether to compute the metric from the first day or not
            if True, then would compute from start
            default is False
        """
        # Getting all possible dates
        computable_dates = self.projection_utils.get_dates()

        computed_dates = self.get_computed_dates()

        # compute for each date
        to_compute: set[float]
        if from_start:
            to_compute = computable_dates
        else:
            to_compute = computable_dates - computed_dates

        # for the computation date
        for date in to_compute:
            try:
                self.local_clustering_computation_wrapper(date=date)
            except Exception as exp:
                logging.error(
                    f"{self.log_prefix}localClustering computation for "
                    f"date: {date}, exp: {exp}"
                )

    def local_clustering_computation_wrapper(self, date: float) -> None:
        """
        a wrapper for local clustering coefficient computation process
        we're doing the projection here and computing on that,
        then we'll drop the pojection

        Parameters:
        ------------
        projection_utils : ProjectionUtils
            the utils needed to get the work done
        guildId : str
            the guild we want the temp relationships
            between its members
        date : float
            timestamp of the relation
        """
        graph_projected_name = f"GraphLocalClustering_{uuid1()}"
        self.projection_utils.project_temp_graph(
            graph_name=graph_projected_name,
            weighted=True,
            date=date,
        )

        # get the results as pandas dataframe
        self.compute_graph_lcc(date=date, graph_name=graph_projected_name)

        # dropping the computed date
        _ = self.gds.run_cypher(
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
            MATCH (:{self.graph_schema.user_label})
                -[r:{self.graph_schema.interacted_in_rel}]->
                (g:{self.graph_schema.platform_label} {{id: $platform_id}})
            WHERE r.localClusteringCoefficient IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = self.projection_utils.get_computed_dates(
            query, platform_id=self.platform_id
        )

        return computed_dates

    def compute_graph_lcc(self, date: float, graph_name: str) -> None:
        """
        compute the localClusteringCoefficient for the given graph
        and write the results back to the nodes

        Parameters:
        ------------
        date : float
            timestamp of the relation
        graph_name : str
            the operation would be done on the graph
        """
        try:
            _ = self.gds.run_cypher(
                f"""
                    CALL gds.localClusteringCoefficient.stream(
                        $graph_name
                    ) YIELD nodeId, localClusteringCoefficient
                    WITH
                        gds.util.asNode(nodeId) as userNode,
                        localClusteringCoefficient
                    MATCH (g:{self.graph_schema.platform_label} {{id: $platform_id}})
                    MERGE (userNode) -[r:{self.graph_schema.interacted_in_rel}  {{date: $date}}]-> (g)
                    SET r.localClusteringCoefficient = localClusteringCoefficient
                    """,
                {
                    "graph_name": graph_name,
                    "platform_id": self.platform_id,
                    "date": date,
                },
            )
        except Exception as exp:
            logging.error(
                f"{self.log_prefix} error in computing localClusteringCoefficient!"
                f" Exception: {exp}"
            )
