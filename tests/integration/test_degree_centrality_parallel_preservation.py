# the nodes of the graph are partially connected
from discord_analyzer.analysis.neo4j_analysis.centrality import Centerality
from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_partially_connected_coeffs():
    """
    5 nodes partially connected
    using two dates: 166 and 167

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
    centrality = Centerality()
    degree_centrality = centrality.compute_degree_centerality(
        guildId=guildId,
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
