# we have nodes of a community is connected to another one
# meaning we have nodes available in more than one community
from tc_analyzer_lib.algorithms.neo4j_analysis.centrality import Centerality
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_multiple_guilds_preserve_parallel():
    """
    5 nodes connected to guild 1234
    2 nodes conected to guild 1235
    using two dates: 166 and 167

    We do not have to have the result of guild 1234 in guild 1235 and vice versa
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
    platform_id1 = "5151515151515"
    platform_id2 = "5151515151516"
    centrality = Centerality(platform_id2, graph_schema)

    user_label = graph_schema.user_label
    platform_label = graph_schema.platform_label
    interacted_with = graph_schema.interacted_with_rel
    is_member = graph_schema.member_relation

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:{user_label}) -[:{is_member}]->(g:{platform_label} {{id: '{platform_id1}'}})
        CREATE (b:{user_label}) -[:{is_member}]->(g)
        CREATE (c:{user_label}) -[:{is_member}]->(g)
        CREATE (d:{user_label}) -[:{is_member}]->(g)
        CREATE (e:{user_label}) -[:{is_member}]->(g)
        CREATE (f2:{user_label})
            -[:{is_member}]->(guild2:{platform_label} {{id: '{platform_id2}'}})
        CREATE (g2:{user_label}) -[:{is_member}]->(guild2)
        SET a.id = "1000"
        SET b.id = "1001"
        SET c.id = "1002"
        SET d.id = "1003"
        SET e.id = "1004"
        SET f2.id = "1005"
        SET g2.id = "1006"
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
        MERGE (f2) -[r13:{interacted_with} {{date: {yesterday}, weight: 3}}]->(g2)
        MERGE (g2) -[r14:{interacted_with} {{date: {yesterday}, weight: 3}}]->(f2)
        SET r.platformId = '{platform_id1}'
        SET r2.platformId = '{platform_id1}'
        SET r3.platformId = '{platform_id1}'
        SET r4.platformId = '{platform_id1}'
        SET r5.platformId = '{platform_id1}'
        SET r6.platformId = '{platform_id1}'
        SET r7.platformId = '{platform_id1}'
        SET r8.platformId = '{platform_id1}'
        SET r9.platformId = '{platform_id1}'
        SET r10.platformId = '{platform_id1}'
        SET r11.platformId = '{platform_id1}'
        SET r12.platformId = '{platform_id1}'
        SET r13.platformId = '{platform_id2}'
        SET r14.platformId = '{platform_id2}'
        """
    )
    degree_centrality = centrality.compute_degree_centerality(
        direction="undirected",
        normalize=False,
        weighted=False,
        preserve_parallel=True,
        from_start=True,
    )
    print("degree_centrality: ", degree_centrality)

    assert "1005" in degree_centrality[yesterday]
    assert "1006" in degree_centrality[yesterday]

    assert degree_centrality[yesterday]["1005"] == 2
    assert degree_centrality[yesterday]["1006"] == 2
