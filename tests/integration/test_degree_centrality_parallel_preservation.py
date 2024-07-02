# the nodes of the graph are partially connected
from tc_analyzer_lib.algorithms.neo4j_analysis.centrality import Centerality
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
    is_member = graph_schema.member_relation

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:{user_label}) -[:{is_member}]->(g:{platform_label} {{guildId: '{platform_id}'}})
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
    centrality = Centerality(platform_id, graph_schema)
    degree_centrality = centrality.compute_degree_centerality(
        direction="undirected",
        normalize=True,
        weighted=False,
        preserve_parallel=False,
        from_start=True,
    )

    print(degree_centrality)
    assert degree_centrality[yesterday]["1000"] == 2 / 3
    assert degree_centrality[today]["1000"] == 1 / 2

    assert degree_centrality[yesterday]["1001"] == 1
    assert degree_centrality[today]["1001"] == 1

    assert degree_centrality[yesterday]["1002"] == 2 / 3
    assert degree_centrality[today]["1002"] == 3 / 4

    assert degree_centrality[yesterday]["1003"] == 1
    assert degree_centrality[today]["1003"] == 1 / 2

    assert "1004" not in degree_centrality[yesterday]
    assert degree_centrality[today]["1004"] == 1 / 4
