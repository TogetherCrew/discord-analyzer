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

        # Getting all possible dates
        computable_dates = projection_utils.get_dates(guildId=guildId)

        computed_dates = self.get_computed_dates(projection_utils, guildId)

        # compute for each date
        to_compute: set[float]
        if from_start:
            to_compute = computable_dates
        else:
            to_compute = computable_dates - computed_dates

        # for the computation date
        for date in to_compute:
            try:
                self.local_clustering_computation_wrapper(
                    projection_utils=projection_utils, guildId=guildId, date=date
                )
            except Exception as exp:
                msg = f"GUILDID: {guildId} "
                logging.error(
                    f"{msg}localClustering computation for date: {date}, exp: {exp}"
                )

    def local_clustering_computation_wrapper(
        self, projection_utils: ProjectionUtils, guildId: str, date: float
    ) -> None:
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
        projection_utils.project_temp_graph(
            guildId=guildId,
            graph_name=graph_projected_name,
            weighted=True,
            date=date,
        )

        # get the results as pandas dataframe
        self.compute_graph_lcc(
            date=date, graph_name=graph_projected_name, guildId=guildId
        )

        # dropping the computed date
        _ = self.gds.run_cypher(
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
        query = """
            MATCH (:DiscordAccount)
                -[r:INTERACTED_IN]->(g:Guild {guildId: $guild_id})
            WHERE r.localClusteringCoefficient IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = projection_utils.get_computed_dates(query, guild_id=guildId)

        return computed_dates

    def compute_graph_lcc(self, date: float, graph_name: str, guildId: str) -> None:
        """
        compute the localClusteringCoefficient for the given graph
        and write the results back to the nodes

        Parameters:
        ------------
        date : float
            timestamp of the relation
        graph_name : str
            the operation would be done on the graph
        guild : str
            the guildId to save the data for it
        """
        msg = f"GUILDID: {guildId}"
        try:
            _ = self.gds.run_cypher(
                f"""
                    CALL gds.localClusteringCoefficient.stream(
                        "{graph_name}"
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
            logging.error(f"{msg} error in computing localClusteringCoefficient, {exp}")
