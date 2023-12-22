# test out local clustering coefficient with all nodes connected
from datetime import datetime, timedelta

import networkx as nx
import numpy as np
from discord_analyzer.analysis.utils.activity import Activity

from .utils.mock_graph import generate_mock_graph, store_mock_data_in_neo4j
from .utils.neo4j_conn import neo4j_setup


def test_network_graph_create():
    community_id = "4321"
    neo4j_ops = neo4j_setup()
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    guildId = "1234"
    acc_names = np.array(["1000", "1001", "1002"])
    graph_dict = {}

    # saving the desired outputs
    desired_outputs = []

    # Generating 1st graph
    np.random.seed(123)
    int_matrix = {}
    int_matrix[Activity.Reply] = np.array(
        [
            [0, 1, 2],
            [0, 0, 3],
            [0, 4, 0],
        ]
    )

    int_matrix[Activity.Mention] = np.array(
        [
            [0, 1, 2],
            [0, 0, 3],
            [0, 4, 0],
        ]
    )

    int_matrix[Activity.Reaction] = np.array(
        [
            [0, 1, 2],
            [0, 0, 3],
            [0, 4, 0],
        ]
    )

    graph = generate_mock_graph(int_matrix, acc_names)

    node_att = {}
    for i, node in enumerate(list(graph)):
        node_att[node] = acc_names[i]

    nx.set_node_attributes(graph, node_att, "acc_name")

    graph_date = datetime.now()
    graph_date_timestamp = graph_date.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    graph_dict[graph_date] = graph

    desired_outputs.extend(
        [
            ["1000", 1, graph_date_timestamp, "1001"],
            ["1000", 2, graph_date_timestamp, "1002"],
            ["1001", 3, graph_date_timestamp, "1002"],
            ["1002", 4, graph_date_timestamp, "1001"],
        ]
    )

    # Generating 2nd graph
    int_matrix = {}
    int_matrix[Activity.Reply] = np.array(
        [
            [0, 0, 1],
            [2, 0, 5],
            [0, 0, 0],
        ]
    )

    int_matrix[Activity.Mention] = np.array(
        [
            [0, 0, 1],
            [2, 0, 5],
            [0, 0, 0],
        ]
    )

    int_matrix[Activity.Reaction] = np.array(
        [
            [0, 0, 1],
            [2, 0, 5],
            [0, 0, 0],
        ]
    )

    graph = generate_mock_graph(int_matrix, acc_names)

    nx.set_node_attributes(graph, node_att, "acc_name")

    graph_date = datetime.now() + timedelta(days=-1)
    graph_date_timestamp = graph_date.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    graph_dict[graph_date] = graph

    desired_outputs.extend(
        [
            ["1000", 1, graph_date_timestamp, "1002"],
            ["1001", 2, graph_date_timestamp, "1000"],
            ["1001", 5, graph_date_timestamp, "1002"],
        ]
    )

    # generating 3rd graph
    int_matrix = {}
    int_matrix[Activity.Reply] = np.array(
        [
            [0, 0, 3],
            [0, 0, 0],
            [1, 0, 0],
        ]
    )
    int_matrix[Activity.Mention] = np.array(
        [
            [0, 0, 3],
            [0, 0, 0],
            [1, 0, 0],
        ]
    )
    int_matrix[Activity.Reaction] = np.array(
        [
            [0, 0, 3],
            [0, 0, 0],
            [1, 0, 0],
        ]
    )

    graph = generate_mock_graph(int_matrix, acc_names)
    nx.set_node_attributes(graph, node_att, "acc_name")

    graph_date = datetime.now() + timedelta(days=-8)
    graph_date_timestamp = graph_date.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    graph_dict[graph_date] = graph

    desired_outputs.extend(
        [
            ["1000", 3, graph_date_timestamp, "1002"],
            ["1002", 1, graph_date_timestamp, "1000"],
        ]
    )

    # DATABASE SAVING

    store_mock_data_in_neo4j(
        graph_dict=graph_dict, guildId=guildId, community_id=community_id
    )

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount) -[:IS_MEMBER] -> (g:Guild {{guildId: '{guildId}'}})
        MATCH (a)-[r:INTERACTED_WITH]-> (b:DiscordAccount)
        RETURN
            a.userId as fromUserId,
            r.weight as weight,
            r.date as date,
            b.userId as toUserId
        """
    )
    print(desired_outputs)
    print(results)
    assert desired_outputs in results.values
