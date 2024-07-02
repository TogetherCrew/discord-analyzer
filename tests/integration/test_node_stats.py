# test out local clustering coefficient with all nodes connected
from tc_analyzer_lib.algorithms.neo4j_analysis.analyzer_node_stats import NodeStats
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_node_stats():
    """
    5 nodes partially connected
    using two dates: 166 and 167

    To see the graph for this test:
    https://miro.com/app/board/uXjVM7GdYqo=/?share_link_id=105382864070
    """
    neo4j_ops = Neo4jOps.get_instance()
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    graph_schema = GraphSchema(platform="discord")
    platform_id = "5151515151515"

    user_label = graph_schema.user_label
    platform_label = graph_schema.platform_label
    interacted_with = graph_schema.interacted_with_rel
    interacted_in = graph_schema.interacted_in_rel
    is_member = graph_schema.member_relation

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0

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

    node_stats = NodeStats(platform_id, graph_schema, threshold=2)
    node_stats.compute_stats(from_start=True)

    # getting the results
    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:{user_label})
             -[r:{interacted_in}] -> (g:{platform_label} {{id: '{platform_id}'}})
        RETURN a.id as userId, r.date as date, r.status as status
        """
    )

    # we had 5 discord accounts and 2 dates for each
    # just the "1004" user did not interact yesterday
    # so 9 status results
    assert len(results) == 9
    print(results)

    results_user0 = results[results["userId"] == "1000"]
    expected_results = [
        ["1000", today, 2],
        ["1000", yesterday, 0],
    ]
    assert expected_results in results_user0.values

    results_user1 = results[results["userId"] == "1001"]
    expected_results = [
        ["1001", today, 1],
        ["1001", yesterday, 1],
    ]
    assert expected_results in results_user1.values

    results_user2 = results[results["userId"] == "1002"]
    expected_results = [
        ["1002", today, 0],
        ["1002", yesterday, 0],
    ]
    assert expected_results in results_user2.values

    results_user3 = results[results["userId"] == "1003"]
    expected_results = [
        ["1003", today, 2],
        ["1004", yesterday, 1],
    ]
    assert expected_results in results_user3.values

    results_user4 = results[results["userId"] == "1004"]
    expected_results = [["1004", today, 0]]
    assert expected_results in results_user4.values
