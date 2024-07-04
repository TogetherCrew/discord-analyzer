# Computation of Neo4j analytics
from typing import Literal

import numpy as np
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Neo4JMetrics:
    def __init__(self) -> None:
        """
        computation of Neo4J metrics

        Parameters:
        ------------
        gds : GraphDataScience
            the GraphDataScience instance to query the DB
        """
        self.gds = Neo4jOps.get_instance().gds

    def compute_degreeCenterality(self, graphProjection, method, configuration=None):
        """
        compute the degree decenterality metrics for the graphProjection

        Parameters:
        -------------
        graphProjection : gds.graph.project
            the graph projection to compute the local Clustering Coefficient on it
        method : str
            whether `stream`, `stats`, `Mutate`, or `write`
            each has a special effect on the database,
           https://neo4j.com/docs/graph-data-science/current/graph-catalog-node-ops/
        configuration : str or list or map
            additional configurations for the gds_operator
            default is `None` meaning no configurations is applied

        Returns:
        ----------
        results : pandas dataframe
            the result of gds.localClusteringCoefficient in pandas dataframe format
        """

        results = self._run_on_method(
            gds_operator=self.gds.degree,
            method=method,
            graphProjection=graphProjection,
            additional_configurations=configuration,
        )

        return results

    def compute_isolated_nodes(
        self, guildId, nodeType="DiscordAccount", relType="INTERACTED"
    ):
        """
        retrieve the isolated nodes for one or more guilds

        Parameters:
        ------------
        guildId : list of str
            string id
            minimum length must be 1
        nodeType : str
            optional, default is `DiscordAccount`
        relType : str
            optional, default is `INTERACTED`
            the relationship that would be assumed to compute the metric

        Returns:
        ---------
        isolated_nodes : pandas dataframe
            the isolated nodes list
        """
        if not isinstance(guildId, list):
            raise ValueError(
                f"guildId should be a list of string! Given type is: {type(guildId)}"
            )
        if len(guildId) < 1:
            msg = "guildId should be a list with minimum length of 1!"
            raise ValueError(f"{msg} Given length is: {len(guildId)}")

        isolated_nodes = self.gds.run_cypher(
            f"""
            MATCH (isolated_nodes:{nodeType}) -[:IS_MEMBER]->(guild:Guild)
            WHERE
                NOT (isolated_nodes)-[:{relType}]-() AND
                guild.guildId IN {guildId}
            RETURN DISTINCT (isolated_nodes).userId AS userId
        """
        )

        return isolated_nodes

    def compute_isolated_nodes_fraction(
        self, guildId, nodeType="DiscordAccount", relType="INTERACTED"
    ):
        """
        retrieve the count isolated nodes divided by all nodes for one or more guilds

        Parameters:
        ------------
        guildId : list of str
            string id
            minimum length must be 1
        nodeType : str
            optional, default is `DiscordAccount`
        relType : str
            optional, default is `INTERACTED`
            the relationship that would be assumed to compute the metric

        Returns:
        ---------
        isolation_fraction : float
            the fraction of isolation in network
        """
        if not isinstance(guildId, list):
            raise ValueError(
                f"guildId should be a list of string! Given type is: {type(guildId)}"
            )
        if len(guildId) < 1:
            msg = "guildId should be a list with minimum length of 1!"
            raise ValueError(f"{msg} Given length is: {len(guildId)}")

        result = self.gds.run_cypher(
            f"""
            MATCH (isolated_nodes:{nodeType}) -[:IS_MEMBER]->(guild:Guild)
            WHERE not (isolated_nodes)-[:{relType}]-() AND guild.guildId in {guildId}
            WITH COUNT(isolated_nodes) * 1.0 as isolated_nodes_count
            MATCH (nodes:DiscordAccount) -[:IS_MEMBER]-> (guild:Guild)
            WHERE guild.guildId in {guildId}

            WITH COUNT(nodes) as all_nodes_count, isolated_nodes_count
            CALL apoc.when(
                all_nodes_count = 0,
                'RETURN 0 AS isolation_fraction',
                'RETURN $isolated_nodes_count / $all_nodes_count AS isolation_fraction',
                {{
                    isolated_nodes_count: isolated_nodes_count,
                    all_nodes_count: all_nodes_count
                }}
            ) YIELD value
            RETURN value.isolation_fraction as isolation_fraction
            """
        )

        # getting the float value from
        # a one row dataframe with column name `isolation_fraction`
        return result["isolation_fraction"].values[0]

    def compute_network_density(
        self, guildId, nodeType="DiscordAccount", relType="INTERACTED"
    ):
        """
        compute network density for one or more guilds

        Parameters:
        ------------
        guildId : list of str
            string id
            minimum length must be 1
        nodeType : str
            optional, default is `DiscordAccount`
        relType : str
            optional, default is `INTERACTED`
            the relationships that would be count to compute the metric

        Returns:
        ---------
        network_density : float
            the fraction of isolation in network
        """
        if not isinstance(guildId, list):
            raise ValueError(
                f"guildId should be a list of string! Given type is: {type(guildId)}"
            )
        if len(guildId) < 1:
            msg = "guildId should be a list with minimum length of 1!"
            raise ValueError(f"{msg} Given length is: {len(guildId)}")

        result = self.gds.run_cypher(
            f"""
            MATCH (nodes:{nodeType}) -[:IS_MEMBER]->(guild:Guild)
            WHERE guild.guildId in {guildId}
            WITH
                COUNT(DISTINCT(nodes)) * 1.0 * (COUNT(DISTINCT(nodes)) - 1) * 2
                AS potential_connection_count
            MATCH (nodes)-[r:{relType}]-()
            WITH COUNT(DISTINCT(r)) * 1.0 AS actual_connection_count,
            potential_connection_count
            RETURN
                actual_connection_count / potential_connection_count
                AS network_density
        """
        )

        # getting the float value from a one row dataframe
        # with column name `network_density`
        return result["network_density"].values[0]

    def compute_decentralization(self, centrality: list[float]) -> float | Literal[-1]:
        """
        Computes degree decentralization score of a graph
        Note: the degreeCenterality must be computed before to comute descenterality

        Parameters:
        -------------
        centrality : list[float]
            list of centrality scores per node

        Returns:
        ----------
        network_decentrality : float
            the decentrality score
        """
        # converting to numpy
        centrality_np = np.array(centrality)

        # get number of non-zero values in list
        n_val_nonzero = len(centrality_np[centrality_np != 0])
        # n_val = float(len(centrality))

        # define denominator
        c_denominator = (n_val_nonzero - 1) * (n_val_nonzero - 2)

        # get max centrality
        c_node_max = max(centrality)

        # sort centrality scores
        c_sorted = sorted(centrality, reverse=True)

        # initate c_numerator at 0
        c_numerator: float = 0.0

        # for each sorted score
        for value in c_sorted:
            # computing over the positive values
            if value != 0:
                # remove normalisation for each value
                c_numerator += c_node_max * (n_val_nonzero - 1) - value * (
                    n_val_nonzero - 1
                )
        if c_denominator != 0:
            # compute network centrality
            network_centrality = float(c_numerator / c_denominator)

            # compute network decentrality
            network_decentrality = 2 * (100 - (network_centrality * 100))
        else:
            # setting `-1`
            network_decentrality = -1

        return network_decentrality

    def _run_on_method(
        self, gds_operator, method, graphProjection, additional_configurations=None
    ):
        """
        run the gds_operation with the method `stream`, `stats`, `Mutate`, or `write`

        Parameters:
        ------------
        gds_operator : gds.#some operation
            the graph datascience operation
        method : str
            whether `stream`, `stats`, `mutate`, or `write`
            each has a special effect on the database,
           https://neo4j.com/docs/graph-data-science/current/graph-catalog-node-ops/
        graphProjection : gds.graph.project
            the graph projection to compute the local Clustering Coefficient on it
        additional_configurations : str or list or map
            additional configurations for the gds_operator
            default is `None` meaning no configurations is used

        Returns:
        ------------
        results : pandas dataframe
            the result of gds operation in pandas dataframe format
        """
        if method == "stream":
            if additional_configurations is not None:
                results = gds_operator.stream(
                    graphProjection, additional_configurations
                )
            else:
                results = gds_operator.stream(graphProjection)

        elif method == "stats":
            if additional_configurations is not None:
                results = gds_operator.stats(graphProjection, additional_configurations)
            else:
                results = gds_operator.stats(graphProjection)

        elif method == "mutate":
            if additional_configurations is not None:
                results = gds_operator.mutate(
                    graphProjection, additional_configurations
                )
            else:
                results = gds_operator.mutate(graphProjection)

        elif method == "write":
            if additional_configurations is not None:
                results = gds_operator.write(graphProjection, additional_configurations)
            else:
                results = gds_operator.write(graphProjection)
        else:
            prefix_msg = "Invalid method name, "
            prefix_msg += "should be either `stream`, `stats`, `mutate`, or `write`"
            raise ValueError(f"{prefix_msg}\n given: {method} ")

        return results
