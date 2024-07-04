from tc_analyzer_lib.algorithms.neo4j_analysis.utils import ProjectionUtils
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_neo4j_projection_utils_get_computed_dates():
    """
    testing the projection utils get_computed_dates
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
        MERGE (a)-[:{interacted_in} {{date: {yesterday}}}]->(g)
        MERGE (a)-[:{interacted_in} {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (b)-[:{interacted_in} {{date: {yesterday}}}]->(g)
        MERGE (b)-[:{interacted_in} {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (c)-[:{interacted_in} {{date: {yesterday}}}]->(g)
        MERGE (c)-[:{interacted_in} {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (d)-[:{interacted_in} {{date: {yesterday}}}]->(g)
        MERGE (e)-[:{interacted_in} {{date: {today}, localClusteringCoefficient: 1}}]->(g)


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
    projection_utils = ProjectionUtils(platform_id, graph_schema)
    computed_dates = projection_utils.get_computed_dates(
        f"""
        MATCH (:{user_label})-[r:{interacted_in}]->(g:{platform_label} {{id: $platform_id}})
        WHERE r.localClusteringCoefficient is NOT NULL
        RETURN r.date as computed_dates
        """,
        platform_id=platform_id,
    )

    print(computed_dates)

    assert computed_dates == {today}
