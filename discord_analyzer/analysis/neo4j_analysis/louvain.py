import logging
from uuid import uuid1

from discord_analyzer.analysis.neo4j_utils.projection_utils import ProjectionUtils

from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Louvain:
    def __init__(self, neo4j_ops: Neo4jOps) -> None:
        """
        louvain algorithm wrapper to compute
        """
        self.neo4j_ops = neo4j_ops

    def compute(self, guild_id: str, from_start: bool = False) -> None:
        """
        compute the louvain modularity score for a guild

        Parameters
        ------------
        guild_id : str
            the guild_id to compute the the algorithm for
        from_start : bool
            whether to compute the metric from the first day or not
            if True, then would compute from start
            default is False
        """
        projection_utils = ProjectionUtils(gds=self.neo4j_ops.gds, guildId=guild_id)

        computable_dates = projection_utils.get_dates(guildId=guild_id)

        # compute for each date
        to_compute: set[float]
        if from_start:
            to_compute = computable_dates
        else:
            computed_dates = self.get_computed_dates(projection_utils, guild_id)
            to_compute = computable_dates - computed_dates

        for date in to_compute:
            try:
                self.louvain_computation_wrapper(projection_utils, guild_id, date)
            except Exception as exp:
                msg = f"GUILDID: {guild_id} "
                logging.error(
                    f"{msg}Louvain Modularity computation for date: {date}, exp: {exp}"
                )

    def louvain_computation_wrapper(
        self, projection_utils: ProjectionUtils, guild_id: str, date: float
    ) -> None:
        """
        a wrapper for louvain modularity computation process
        we're doing the projection here and computing on that,
        then we'll drop the pojection

        Parameters:
        ------------
        projection_utils : ProjectionUtils
            the utils needed to get the work done
        guild_id : str
            the guild we want the temp relationships
            between its members
        date : float
            timestamp of the relation
        """
        graph_projected_name = f"GraphLouvain_{uuid1()}"
        projection_utils.project_temp_graph(
            guildId=guild_id,
            graph_name=graph_projected_name,
            weighted=True,
            date=date,
            relation_direction="NATURAL",
        )

        # get the results as pandas dataframe
        self.compute_graph_louvain(
            date=date, graph_name=graph_projected_name, guild_id=guild_id
        )

        # dropping the computed date
        _ = self.neo4j_ops.gds.run_cypher(
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
            MATCH (g:Guild {{guildId: '{guildId}'}})
                -[r:HAVE_METRICS]->(g)
            WHERE r.louvainModularityScore IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = projection_utils.get_computed_dates(query)

        return computed_dates

    def compute_graph_louvain(
        self, date: float, graph_name: str, guild_id: str
    ) -> None:
        """
        compute louvain algorithm for the projected graph and
        save the results back into db

        Parameters:
        ------------
        date : float
            timestamp of the relation
        graph_name : str
            the operation would be done on the graph
        guild_id : str
            the guild_id to save the data for it
        """
        msg = f"GUILDID: {guild_id}"
        try:
            _ = self.neo4j_ops.gds.run_cypher(
                f"""
                    CALL gds.louvain.stats("{graph_name}") 
                    YIELD modularity
                    WITH modularity
                    MATCH (g:Guild {{guildId: '{guild_id}'}})
                    MERGE (g) -[r:HAVE_METRICS {{
                        date: {date}
                    }}]-> (g)
                    SET r.louvainModularityScore = modularity
                    """
            )
        except Exception as exp:
            logging.error(
                f"{msg} Error in computing louvain modularity algorithm, {exp}"
            )
