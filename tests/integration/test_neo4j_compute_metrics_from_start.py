import numpy as np
from tc_analyzer_lib.metrics.neo4j_analytics import Neo4JAnalytics
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_neo4j_compute_metrics_from_start():
    """
    test with default behaviour

    test whether the averageClusteringCoefficeint
    and decentralization scores are available in guild node
    and localClustetingCoefficient is available in DiscordAccount nodes
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

    analytics = Neo4JAnalytics(platform_id, graph_schema)

    analytics.compute_metrics(from_start=True)

    accounts_result = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:{user_label}) -[r:{interacted_in}]-> (g:{platform_label} {{id: '{platform_id}'}})
        RETURN
            a.id AS userId,
            r.date AS date,
            r.localClusteringCoefficient AS localClusteringCoefficient,
            r.status AS status
        """
    )

    # we don't have 1004 interacting on yesterday (1689193800.0)
    assert len(accounts_result.values) == 9

    for _, row in accounts_result.iterrows():
        print(row)
        lcc = row["localClusteringCoefficient"]
        date = row["date"]
        userId = row["userId"]
        status = row["status"]

        assert userId is not None

        assert date in [yesterday, today]
        assert bool(np.isnan(lcc)) is False
        assert lcc is not None

        assert status is not None

        assert bool(np.isnan(status)) is False

    guild_results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:{platform_label} {{id: '{platform_id}'}}) -[r:HAVE_METRICS]->(g)
        RETURN
            r.date as date,
            g.id as platformId,
            r.decentralizationScore as decentralizationScore
        """
    )

    # for 2 dates
    assert len(guild_results.values) == 2
    for _, row in guild_results.iterrows():
        print(row)
        assert row["date"] in [yesterday, today]
        assert row["platformId"] == platform_id
        assert row["decentralizationScore"] is not None
        assert bool(np.isnan(row["decentralizationScore"])) is False
