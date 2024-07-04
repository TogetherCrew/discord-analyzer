from tc_analyzer_lib.algorithms.neo4j_analysis.louvain import Louvain
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_louvain_get_computed_dates_empty_data():
    """
    test with empty data for getting the computed dates
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
    louvain = Louvain(platform_id, graph_schema)

    computed_dates = louvain.get_computed_dates()

    assert computed_dates == set()

    # clean-up
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")


def test_louvain_get_computed_dates_empty_data_with_have_metrics_relation():
    """
    test with empty data for getting the computed dates
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
        MERGE (g)-[:HAVE_METRICS {{date: {yesterday}}}]->(g)
        SET r.platformId = '{platform_id}'
        SET r2.platformId = '{platform_id}'
        SET r3.platformId = '{platform_id}'
        SET r4.platformId = '{platform_id}'
        """
    )
    louvain = Louvain(platform_id, graph_schema)
    computed_dates = louvain.get_computed_dates()

    assert computed_dates == set()
    # clean-up
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")


def test_louvain_get_computed_dates_one_data():
    """
    test with empty data for getting the computed dates
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
        MERGE (g)-[:HAVE_METRICS {{date: {yesterday}, louvainModularityScore: 0.0}}]->(g)
        SET r.platformId = '{platform_id}'
        SET r2.platformId = '{platform_id}'
        SET r3.platformId = '{platform_id}'
        SET r4.platformId = '{platform_id}'
        """
    )
    louvain = Louvain(platform_id, graph_schema)
    computed_dates = louvain.get_computed_dates()

    assert computed_dates == {yesterday}
    # clean-up
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")
