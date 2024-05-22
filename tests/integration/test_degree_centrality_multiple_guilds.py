# we have nodes of a community is connected to another one
# meaning we have nodes available in more than one community
from discord_analyzer.analysis.neo4j_analysis.centrality import Centerality
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_multiple_guilds():
    """
    5 nodes connected to guild 1234
    2 nodes conected to guild 1235
    using two dates: 166 and 167

    We do not have to have the result of guild 1234 in guild 1235 and vice versa
    To see more info for this test:
    https://miro.com/app/board/uXjVM7GdYqo=/?share_link_id=105382864070
    """
    guildId = "1234"
    neo4j_ops = Neo4jOps.get_instance()

    # deleting all data
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0

    guildId = "1234"
    guildId2 = "1235"

    # creating some nodes with data
    neo4j_ops.gds.run_cypher(
        f"""
        CREATE (a:DiscordAccount) -[:IS_MEMBER]->(g:Guild {{guildId: '{guildId}'}})
        CREATE (b:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (c:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (d:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (e:DiscordAccount) -[:IS_MEMBER]->(g)
        CREATE (f2:DiscordAccount)
            -[:IS_MEMBER]->(guild2:Guild {{guildId: '{guildId2}'}})
        CREATE (g2:DiscordAccount) -[:IS_MEMBER]->(guild2)
        SET a.userId = "1000"
        SET b.userId = "1001"
        SET c.userId = "1002"
        SET d.userId = "1003"
        SET e.userId = "1004"
        SET f2.userId = "1005"
        SET g2.userId = "1006"
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
        MERGE (f2) -[r13:INTERACTED_WITH {{date: {yesterday}, weight: 3}}]->(g2)
        MERGE (g2) -[r14:INTERACTED_WITH {{date: {yesterday}, weight: 3}}]->(f2)
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
        SET r13.guildId = '{guildId2}'
        SET r14.guildId = '{guildId2}'
        """
    )
    centrality = Centerality(neo4j_ops)
    degree_centrality = centrality.compute_degree_centerality(
        guildId=guildId2,
        direction="undirected",
        normalize=True,
        weighted=False,
        preserve_parallel=True,
        from_start=True,
    )
    print("degree_centrality: ", degree_centrality)

    assert today not in degree_centrality
    assert "1000" not in degree_centrality[yesterday]
    assert "1001" not in degree_centrality[yesterday]
    assert "1002" not in degree_centrality[yesterday]
    assert "1003" not in degree_centrality[yesterday]
    assert "1004" not in degree_centrality[yesterday]

    assert "1005" in degree_centrality[yesterday]
    assert "1006" in degree_centrality[yesterday]
