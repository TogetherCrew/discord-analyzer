# test out local clustering coefficient with all nodes connected
from tc_analyzer_lib.algorithms.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_all_connected_coeffs():
    """
    3 nodes all connected
    using two dates: 166 and 167
    in date 166 the coeffs are 1.0
    and in date 167 the coeffs are 0.0

    To see more info for this test:
    https://miro.com/app/board/uXjVM7GdYqo=/?share_link_id=105382864070
    """
    neo4j_ops = Neo4jOps.get_instance()
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0
    graph_schema = GraphSchema(platform="discord")
    platform_id = "5151515151515"

    user_label = graph_schema.user_label
    platform_label = graph_schema.platform_label
    interacted_with = graph_schema.interacted_with_rel
    interacted_in = graph_schema.interacted_in_rel
    is_member = graph_schema.member_relation
    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:{user_label}) -[:{is_member}]->(g:{platform_label} {{id: '{platform_id}'}})
        CREATE (b:{user_label}) -[:{is_member}]->(g)
        CREATE (c:{user_label}) -[:{is_member}]->(g)
        SET a.id = "1000"
        SET b.id = "1001"
        SET c.id = "1002"
        MERGE (a) -[r:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
        MERGE (a) -[r2:{interacted_with} {{weight: 2, date: {today}}}]->(b)
        MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
        MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
        SET r.platformId = '{platform_id}'
        SET r2.platformId = '{platform_id}'
        SET r3.platformId = '{platform_id}'
        SET r4.platformId = '{platform_id}'
        """
    )
    lcc = LocalClusteringCoeff(platform_id, graph_schema)
    lcc.compute(from_start=True)

    # getting the results
    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:{user_label}) -[r:{interacted_in}]-> (:{platform_label} {{id: '{platform_id}'}})
        RETURN
            a.id as userId,
            r.date as date,
            r.localClusteringCoefficient as lcc
        """
    )

    user0_id = "1000"
    expected_results_user0 = [
        [user0_id, yesterday, 1.0],
        [user0_id, today, 0.0],
    ]
    assert expected_results_user0 in results[results.userId == user0_id].values

    user1_id = "1001"
    expected_results_user1 = [
        [user1_id, yesterday, 1.0],
        [user1_id, today, 0.0],
    ]
    assert expected_results_user1 in results[results.userId == user1_id].values

    user2_id = "1002"
    expected_results_user2 = [
        [user2_id, yesterday, 1.0],
        [user2_id, today, 0.0],
    ]
    assert expected_results_user2 in results[results.userId == user2_id].values
