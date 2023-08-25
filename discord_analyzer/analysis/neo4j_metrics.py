import os

from dotenv import load_dotenv

from discord_analyzer.DB_operations.neo4j_utils import Neo4jUtils

from discord_analyzer.analysis.neo4j_utils.compute_metrics import (  # isort: skip
    Neo4JMetrics,
)


def degree_centrality(
    gds,
    neo4j_analytics,
    use_names=False,
    drop_projection=True,
    method="stream",
    node="DiscordAccount",
    relationship="INTERACTED",
    relationship_orientation="NATURAL",
    parallel_relationship=False,
):
    """
    a sample function to show how to compute DegreeCenterality using neo4j_utils
    Note: this function does not assume the relation over time


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
    drop_projection : bool
        drop the graph projection
        default is True, which means the graph projections
          will be dropped after metric computation
        **Note:** Must drop the projection to be able to update results,
        make it False if you want do something experimental.
    method : str
        whether `stream`, `stats`, `Mutate`, or `write`, default is `stream`
        each has a special effect on the database,
        see: https://neo4j.com/docs/graph-data-science/current/graph-catalog-node-ops/
    node : str
        the node name we're computing the degree centrality for
        NOTE: Important to have the node exactly like it is saved in DB.
    relationship : str
        the relationship name we're computing the degree centrality for
    relationship_orientation : str
        the relationship orientation to be assumed
        either `NATURAL`, `REVERSE`, or `UNDIRECTED`
    parallel_relationship : bool
        whether to assume parallel relationship as one or the real count
        if False, then for relationship like A -> B
        and B->A the degree centrality of A and B will be 2
        else the degree centrality of A and B will be 1

    Returns:
    ---------
    results : pandas dataframe
        the results of metrics in pandas dataframe format
    """

    if relationship_orientation not in ["NATURAL", "REVERSE", "UNDIRECTED"]:
        msg_prefix = "Wrong relationship orientation given"
        msg_prefix += "should be either `NATURAL`, `REVERSE`, or `UNDIRECTED`!"
        raise ValueError(f"{msg_prefix} Entered: {relationship_orientation}")

    # compute the total weight of each INTERACTED relationship
    gds.run_cypher(
        """MATCH (a:DiscordAccount) -[r:INTERACTED]-(:DiscordAccount)
    SET r.total_weight= REDUCE(total=0, weight in r.weights | total + weight);"""
    )

    # make the relationship projection configs
    relationship_projection = {}

    if parallel_relationship:
        relationship_projection[f"{relationship}"] = {
            "properties": {"total_weight": {"aggregation": "SUM"}},
            "orientation": f"{relationship_orientation}",
        }
    else:
        relationship_projection[f"{relationship}"] = {
            "orientation": f"{relationship_orientation}",
            "properties": ["total_weight"],
        }

    # first we have to apply the projection (will be saved in server memory)
    G, _ = gds.graph.project("MyGraph", node, relationship_projection)

    configuration = None
    if method == "write":
        configuration = {"relationshipWeightProperty": "total_weight"}

    # get the results as pandas dataframe
    results = neo4j_analytics.compute_degreeCenterality(
        G, method=method, configuration=configuration
    )

    if use_names:
        results["userId"] = results["nodeId"].apply(
            lambda nodeId: dict(gds.util.asNode(nodeId))["userId"]
        )

    if drop_projection:
        _ = gds.graph.drop(G)

    return results


def decenterialization_score(neo4j_analytics, centrality_scores):
    """
    a sample function to show how the network decentrality can be computed

    Parameters:
    ------------
    neo4j_analytics : Neo4JMetrics object
        our written Neo4JMetrics class instance
    centrality_scores : array
        array of user centrality scores

    Returns:
    ---------
    network_decentrality : float
        the decentrality score of network
    """
    network_decentrality = neo4j_analytics.compute_decentralization(centrality_scores)

    return network_decentrality


if __name__ == "__main__":
    load_dotenv()

    protocol = os.getenv("NEO4J_PROTOCOL")
    host = os.getenv("NEO4J_HOST")
    port = os.getenv("NEO4J_PORT")
    db_name = os.getenv("NEO4J_DB")

    url = f"{protocol}://{host}:{port}"

    user, password = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))

    neo4j_utils = Neo4jUtils()
    neo4j_utils.set_neo4j_db_info(db_name, url, user, password)
    neo4j_utils.neo4j_database_connect()

    gds = neo4j_utils.gds

    neo4j_analytics = Neo4JMetrics(gds)

    results_degreeCenterality = degree_centrality(
        gds,
        neo4j_analytics=neo4j_analytics,
        use_names=True,
        drop_projection=True,
        method="stream",
        node="DiscordAccount",
        relationship="INTERACTED",
        relationship_orientation="UNDIRECTED",
        parallel_relationship=True,
    )

    # finding the output relationship counts from a node
    results_degreeCentrality_OUT = degree_centrality(
        gds,
        neo4j_analytics=neo4j_analytics,
        use_names=True,
        drop_projection=True,
        method="stream",
        node="DiscordAccount",
        relationship="INTERACTED",
        relationship_orientation="NATURAL",
        # parallel_relationship = True
    )
    # finding the input relationship counts to a node
    results_degreeCentrality_IN = degree_centrality(
        gds,
        neo4j_analytics=neo4j_analytics,
        use_names=True,
        drop_projection=True,
        method="stream",
        node="DiscordAccount",
        relationship="INTERACTED",
        relationship_orientation="REVERSE",
        # parallel_relationship = True
    )

    # what guilds to find isolated nodes
    guildId_arr = ["123456789101112", "993163081939165234", "1012430565959553145"]
    results_isolated_discordNodes = neo4j_analytics.compute_isolated_nodes(
        guildId=guildId_arr
    )
    results_isolation_fraction = neo4j_analytics.compute_isolated_nodes_fraction(
        guildId=guildId_arr
    )
    results_network_density = neo4j_analytics.compute_network_density(
        guildId=guildId_arr
    )

    # adding the scores in and scores out
    # to pandas dataframe of `results_degreeCenterality`
    results_degreeCenterality["score_in"] = results_degreeCentrality_IN["score"]
    results_degreeCenterality["score_out"] = results_degreeCentrality_OUT["score"]
    results_degreeCenterality["score_undirected"] = results_degreeCenterality["score"]

    # normalizing undirected scores
    results_degreeCenterality[
        "normalized_score_undirected"
    ] = results_degreeCenterality["score"] / sum(
        results_degreeCenterality["score"].values > 0
    )
    # the normalization over positive score_out
    results_degreeCenterality["normalized_score_out"] = results_degreeCenterality[
        "score_out"
    ] / sum(results_degreeCenterality["score_out"].values > 0)
    # the normalization over positive score_in
    results_degreeCenterality["normalized_score_in"] = results_degreeCenterality[
        "score_in"
    ] / sum(results_degreeCenterality["score_in"].values > 0)

    results_decentralityScore = decenterialization_score(
        neo4j_analytics=neo4j_analytics,
        centrality_scores=results_degreeCenterality[
            "normalized_score_undirected"
        ].values,
    )

    print("------------------ Degree Centerality ------------------")
    print(results_degreeCenterality, "\n")

    print("------------------ Network Decentrality Score ------------------")
    print(results_decentralityScore, "\n")

    print("------------------ Isolated Nodes ------------------")
    print(f"Isolated Nodes in guilds: {guildId_arr}")
    print(results_isolated_discordNodes, "\n")
    print("Isolation fraction: ", results_isolation_fraction, "\n")

    print("------------------ Network Density ------------------")
    print(f"Network Density for guilds: {guildId_arr}")
    print(results_network_density)
