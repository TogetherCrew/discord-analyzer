import numpy as np

from discord_analyzer.analyzer.neo4j_analytics import Neo4JAnalytics

from .utils.neo4j_conn import neo4j_setup


def test_guild_results_available():
    """
    test with default behaviour

    test whether the averageClusteringCoefficeint
    and decentralization scores are available in guild node
    and localClustetingCoefficient is available in DiscordAccount nodes
    """
    neo4j_ops = neo4j_setup()
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

    analytics = Neo4JAnalytics(neo4j_ops)

    analytics.compute_metrics(guildId=guildId, from_start=False)

    accounts_result = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount) -[r:INTERACTED_IN]-> (g:Guild {{guildId: '{guildId}'}})
        MATCH (g) -[r2:HAVE_METRICS]->(g)
        RETURN
            a.userId AS userId,
            r.date AS date,
            r.localClusteringCoefficient AS localClusteringCoefficient,
            r.status AS status
        """
    )

    for _, row in accounts_result.iterrows():
        print(row)
        assert row["userId"] is not None
        assert row["date"] in [yesterday, today]
        assert bool(np.isnan(row["localClusteringCoefficient"])) is False

    guild_results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:Guild {{guildId: '{guildId}'}}) -[r:HAVE_METRICS]->(g)
        RETURN
            r.date as date,
            g.guildId as guildId,
            r.decentralizationScore as decentralizationScore
        """
    )
    for _, row in guild_results.iterrows():
        print(row)
        assert row["date"] in [yesterday, today]
        assert row["guildId"] == guildId
        assert bool(np.isnan(row["decentralizationScore"])) is False

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:Guild {{guildId: '{guildId}'}})-[r:HAVE_METRICS]->(g)
        RETURN r.date as date, r.louvainModularityScore as modularityScore
        """
    )

    assert len(results) == 2
    print(results)
    assert results["date"].iloc[0] in [yesterday, today]
    assert results["date"].iloc[1] in [yesterday, today]
    assert results["modularityScore"].iloc[0] is not None
    assert results["modularityScore"].iloc[1] is not None
