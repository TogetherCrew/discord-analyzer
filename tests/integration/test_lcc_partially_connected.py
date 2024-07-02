# the nodes of the graph are partially connected
from tc_analyzer_lib.algorithms.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_partially_connected_coeffs():
    """
    5 nodes partially connected
    using two dates: 166 and 167

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
        CREATE (d:{user_label}) -[:{is_member}]->(g)
        CREATE (e:{user_label}) -[:{is_member}]->(g)
        SET a.id = "1000"
        SET b.id = "1001"
        SET c.id = "1002"
        SET d.id = "1003"
        SET e.id = "1004"
        MERGE (a) -[r:{interacted_with} {{date: {yesterday}, weight: 1}}]->(b)
        MERGE (a) -[r2:{interacted_with} {{date: {today}, weight: 2}}]->(b)
        MERGE (a) -[r3:{interacted_with} {{date: {yesterday}, weight: 3}}]->(d)
        MERGE (c) -[r4:{interacted_with} {{date: {yesterday}, weight: 2}}]->(b)
        MERGE (c) -[r5:{interacted_with} {{date: {today}, weight: 1}}]->(b)
        MERGE (c) -[r6:{interacted_with} {{date: {yesterday}, weight: 2}}]->(d)
        MERGE (d) -[r7:{interacted_with} {{date: {yesterday}, weight: 1}}]->(b)
        MERGE (c) -[r8:{interacted_with} {{date: {today}, weight: 2}}]->(a)
        MERGE (d) -[r9:{interacted_with} {{date: {today}, weight: 1}}]->(c)
        MERGE (b) -[r10:{interacted_with} {{date: {today}, weight: 2}}]->(d)
        MERGE (d) -[r11:{interacted_with} {{date: {today}, weight: 1}}]->(c)
        MERGE (e) -[r12:{interacted_with} {{date: {today}, weight: 3}}]->(b)

        SET r.platformId = '{platform_id}'
        SET r2.platformId = '{platform_id}'
        SET r3.platformId = '{platform_id}'
        SET r4.platformId = '{platform_id}'
        SET r5.platformId = '{platform_id}'
        SET r6.platformId = '{platform_id}'
        SET r7.platformId = '{platform_id}'
        SET r8.platformId = '{platform_id}'
        SET r9.platformId = '{platform_id}'
        SET r10.platformId = '{platform_id}'
        SET r11.platformId = '{platform_id}'
        SET r12.platformId = '{platform_id}'
        """
    )
    lcc = LocalClusteringCoeff(platform_id, graph_schema)
    lcc.compute()

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
    print(results.values)

    user0_id = "1000"
    expected_results_user0 = [
        [user0_id, yesterday, 1.0],
        [user0_id, today, 1.0],
    ]
    assert expected_results_user0 in results[results.userId == user0_id].values

    user1_id = "1001"
    expected_results_user1 = [
        [user1_id, yesterday, 2 / 3],
        [user1_id, today, 1 / 3],
    ]
    assert expected_results_user1 in results[results.userId == user1_id].values

    user2_id = "1002"
    expected_results_user2 = [
        [user2_id, yesterday, 1],
        [user2_id, today, 2 / 3],
    ]
    assert expected_results_user2 in results[results.userId == user2_id].values

    user3_id = "1003"
    expected_results_user3 = [
        [user3_id, yesterday, 2 / 3],
        [user3_id, today, 1],
    ]
    assert expected_results_user3 in results[results.userId == user3_id].values

    user4_id = "1003"
    expected_results_user4 = [
        [user4_id, yesterday, 2 / 3],
        [user4_id, today, 1],
    ]
    assert expected_results_user4 in results[results.userId == user4_id].values
