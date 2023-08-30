import logging
from uuid import uuid1

from discord_analyzer.analysis.neo4j_utils.projection_utils import ProjectionUtils
from graphdatascience import GraphDataScience


class LocalClusteringCoeff:
    def __init__(self, gds: GraphDataScience) -> None:
        self.gds = gds

    def compute(self, guildId: str, from_start: bool = False) -> None:
        """
        computing the localClusteringCoefficient
        per date of each interaction and saving them in nodes


        Parameters:
        ------------
        gds : GraphDataScience
            the python GraphDataScience instance
        neo4j_analytics : Neo4JMetrics object
            our written Neo4JMetrics class instance
        use_names : bool
            whether to add user names to results
            if True, the userId will be added alongside nodeId in output
            default is False
        from_start : bool
            whether to compute the metric from the first day or not
            if True, then would compute from start
            default is False

        Returns:
        ---------
        `None`
        """
        projection_utils = ProjectionUtils(gds=self.gds, guildId=guildId)

        graph_projected_name = f"GraphLocalClustering_{uuid1()}"

        # first we have to apply the projection (will be saved in server memory)
        projection_utils.project_temp_graph(
            guildId=guildId,
            graph_name=graph_projected_name,
        )

        # Getting all possible dates
        computable_dates = projection_utils.get_dates(guildId=guildId)

        computed_dates = self.get_computed_dates(projection_utils, guildId)

        # compute for each date
        to_compute = None
        if from_start:
            to_compute = computable_dates
        else:
            to_compute = computable_dates - computed_dates

        # for the computation date
        for date in to_compute:
            subgraph_name = f"SubGraphLocalClustering_{uuid1()}"

            projection_utils.project_subgraph_per_date(
                graph_name=graph_projected_name, subgraph_name=subgraph_name, date=date
            )

            # get the results as pandas dataframe
            self.compute_sub_graph_lcc(
                date=date, subgraph_name=subgraph_name, guildId=guildId
            )

            # dropping the computed date
            _ = self.gds.run_cypher(
                f"""
                CALL gds.graph.drop("{subgraph_name}")
                """
            )

        # drop the original graph projection
        self.gds.run_cypher(
            f"""
            CALL gds.graph.drop("{graph_projected_name}")
            """
        )

    def get_computed_dates(
        self, projection_utils: ProjectionUtils, guildId: str
    ) -> set[float]:
        """
        get localClusteringCoeff computed dates

        Parameters:
        ------------
        guildId : str
            the guild we want the temp relationships
            between its members
        projection_utils : ProjectionUtils
            the utils needed to get the work done

        Returns:
        ----------
        computed_dates : set[float]
            the computation dates
        """
        # getting the dates computed before
        query = f"""
            MATCH (:DiscordAccount)
                -[r:INTERACTED_IN]->(g:Guild {{guildId: '{guildId}'}})
            RETURN r.date as computed_dates, r.localClusteringCoefficient as lcc
            """
        computed_dates = projection_utils.get_computed_dates(query)

        return computed_dates

    def compute_sub_graph_lcc(
        self, date: float, subgraph_name: str, guildId: str
    ) -> None:
        """
        compute the localClusteringCoefficient for the subgraph
        and write the results back to the nodes

        Parameters:
        ------------
        date : float
            timestamp of the relation
        subgraph_name : str
            the operation would be done on the subgraph
        """
        try:
            _ = self.gds.run_cypher(
                f"""
                    // Doing the operation for the day
                    // and saving the localClustering Coeffs
                    CALL gds.localClusteringCoefficient.stream(
                        "{subgraph_name}"
                    ) YIELD nodeId, localClusteringCoefficient
                    WITH
                        gds.util.asNode(nodeId) as userNode,
                        localClusteringCoefficient
                    MATCH (g:Guild {{guildId: '{guildId}'}})
                    MERGE (userNode) -[r:INTERACTED_IN  {{date: {date}}}]-> (g)
                    SET r.localClusteringCoefficient = localClusteringCoefficient
                    """
            )
        except Exception as exp:
            logging.error("Cypher execution in computing localClusteringCoefficient")
            logging.error(f"Exception is: {exp}")
