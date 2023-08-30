from datetime import datetime, timedelta

from discord_analyzer.analyzer.neo4j_analytics import Neo4JAnalytics

from .utils.neo4j_conn import neo4j_setup


def test_avg_clustering_coeff():
    """
    test scaling of the avgClusteringCoefficient (a.k.a fragmentation score)
    """
    neo4j_ops = neo4j_setup()

    neo4j_analytics = Neo4JAnalytics(neo4j_ops)
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0
    past_window_date = (
        datetime.fromtimestamp(yesterday) - timedelta(days=1)
    ).timestamp()

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

    neo4j_analytics.compute_local_clustering_coefficient(
        guildId=guildId, from_start=False
    )
    fragmentation_score = neo4j_analytics.compute_fragmentation_score(
        guildId=guildId,
        past_window_date=past_window_date,
        scale_fragmentation_score=200,
    )

    for score in fragmentation_score:
        if score["date"] == yesterday:
            assert score["fragmentation_score"] - 133.3333 < 0.1
        elif score["date"] == today:
            assert score["fragmentation_score"] - 120 < 0.1
        else:
            print(score["date"])
            # is shouldn't ever get here since
            # we do compute for just two periods
            assert False is True
