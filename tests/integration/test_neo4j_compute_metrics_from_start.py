import numpy as np
from discord_analyzer.analyzer.neo4j_analytics import Neo4JAnalytics
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
    guildId = "1234"

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:DiscordAccount) -[:IS_MEMBER]->(g:Guild {{guildId: '{guildId}'}})
        CREATE (b:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (c:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (d:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (e:DiscordAccount) -[:IS_MEMBER]->(g)
        SET a.userId = "1000"
        SET b.userId = "1001"
        SET c.userId = "1002"
        SET d.userId = "1003"
        SET e.userId = "1004"
        MERGE (a) -[r:INTERACTED_WITH {{date: {yesterday}, weight: 1}}]->(b)
        MERGE (a) -[r2:INTERACTED_WITH {{date: {today}, weight: 2}}]->(b)
        MERGE (a) -[r3:INTERACTED_WITH {{date: {yesterday}, weight: 3}}]->(d)
        MERGE (c) -[r4:INTERACTED_WITH {{date: {yesterday}, weight: 2}}]->(b)
        MERGE (c) -[r5:INTERACTED_WITH {{date: {today}, weight: 1}}]->(b)
        MERGE (c) -[r6:INTERACTED_WITH {{date: {yesterday}, weight: 2}}]->(d)
        MERGE (d) -[r7:INTERACTED_WITH {{date: {yesterday}, weight: 1}}]->(b)
        MERGE (c) -[r8:INTERACTED_WITH {{date: {today}, weight: 2}}]->(a)
        MERGE (d) -[r9:INTERACTED_WITH {{date: {today}, weight: 1}}]->(c)
        MERGE (b) -[r10:INTERACTED_WITH {{date: {today}, weight: 2}}]->(d)
        MERGE (d) -[r11:INTERACTED_WITH {{date: {today}, weight: 1}}]->(c)
        MERGE (e) -[r12:INTERACTED_WITH {{date: {today}, weight: 3}}]->(b)

        SET r.guildId = '{guildId}'
        SET r2.guildId = '{guildId}'
        SET r3.guildId = '{guildId}'
        SET r4.guildId = '{guildId}'
        SET r5.guildId = '{guildId}'
        SET r6.guildId = '{guildId}'
        SET r7.guildId = '{guildId}'
        SET r8.guildId = '{guildId}'
        SET r9.guildId = '{guildId}'
        SET r10.guildId = '{guildId}'
        SET r11.guildId = '{guildId}'
        SET r12.guildId = '{guildId}'
        """
    )

    analytics = Neo4JAnalytics()

    analytics.compute_metrics(guildId=guildId, from_start=True)

    accounts_result = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount) -[r:INTERACTED_IN]-> (g:Guild {{guildId: '{guildId}'}})
        RETURN
            a.userId AS userId,
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
        MATCH (g:Guild {{guildId: '{guildId}'}}) -[r:HAVE_METRICS]->(g)
        RETURN
            r.date as date,
            g.guildId as guildId,
            r.decentralizationScore as decentralizationScore
        """
    )

    # for 2 dates
    assert len(guild_results.values) == 2
    for _, row in guild_results.iterrows():
        print(row)
        assert row["date"] in [yesterday, today]
        assert row["guildId"] == guildId
        assert row["decentralizationScore"] is not None
        assert bool(np.isnan(row["decentralizationScore"])) is False
