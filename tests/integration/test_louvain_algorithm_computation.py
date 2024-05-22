from discord_analyzer.analysis.neo4j_analysis.louvain import Louvain

from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_louvain_algorithm_available_data():
    """
    test the louvain algorithm with some nodes connected
    """
    neo4j_ops = Neo4jOps.get_instance()
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0
    guild_id = "1234"

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:DiscordAccount) -[:IS_MEMBER]->(g:Guild {{guildId: '{guild_id}'}})
        CREATE (b:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (c:DiscordAccount) -[:IS_MEMBER]->(g)
        SET a.userId = "1000"
        SET b.userId = "1001"
        SET c.userId = "1002"
        MERGE (a) -[r:INTERACTED_WITH {{weight: 1, date: {yesterday}}}]->(b)
        MERGE (a) -[r2:INTERACTED_WITH {{weight: 2, date: {today}}}]->(b)
        MERGE (a) -[r3:INTERACTED_WITH {{weight: 3, date: {yesterday}}}]->(c)
        MERGE (b) -[r4:INTERACTED_WITH {{weight: 2, date: {yesterday}}}]->(c)
        SET r.guildId = '{guild_id}'
        SET r2.guildId = '{guild_id}'
        SET r3.guildId = '{guild_id}'
        SET r4.guildId = '{guild_id}'
        """
    )
    louvain = Louvain(neo4j_ops)

    louvain.compute(guild_id=guild_id, from_start=False)

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:Guild {{guildId: '{guild_id}'}})-[r:HAVE_METRICS]->(g)
        RETURN r.date as date, r.louvainModularityScore as modularityScore
        """
    )

    assert len(results) == 2
    assert results["date"].iloc[0] in [yesterday, today]
    assert results["date"].iloc[1] in [yesterday, today]


def test_louvain_algorithm_more_available_data():
    """
    test the louvain algorithm with some more data available
    """
    neo4j_ops = Neo4jOps.get_instance()
    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0
    guild_id = "1234"

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:DiscordAccount) -[:IS_MEMBER]->(g:Guild {{guildId: '{guild_id}'}})
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

        SET r.guildId = '{guild_id}'
        SET r2.guildId = '{guild_id}'
        SET r3.guildId = '{guild_id}'
        SET r4.guildId = '{guild_id}'
        SET r5.guildId = '{guild_id}'
        SET r6.guildId = '{guild_id}'
        SET r7.guildId = '{guild_id}'
        SET r8.guildId = '{guild_id}'
        SET r9.guildId = '{guild_id}'
        SET r10.guildId = '{guild_id}'
        SET r11.guildId = '{guild_id}'
        SET r12.guildId = '{guild_id}'
        """
    )
    louvain = Louvain(neo4j_ops)

    louvain.compute(guild_id=guild_id, from_start=False)

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:Guild {{guildId: '{guild_id}'}})-[r:HAVE_METRICS]->(g)
        RETURN r.date as date, r.louvainModularityScore as modularityScore
        """
    )
    print(results)
    assert len(results) == 2
    assert results["date"].iloc[0] in [yesterday, today]
    assert results["date"].iloc[1] in [yesterday, today]
