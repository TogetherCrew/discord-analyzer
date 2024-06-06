from discord_analyzer.analysis.neo4j_analysis.utils import ProjectionUtils
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
        MERGE (a)-[:INTERACTED_IN {{date: {yesterday}}}]->(g)
        MERGE (a)-[:INTERACTED_IN {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (b)-[:INTERACTED_IN {{date: {yesterday}}}]->(g)
        MERGE (b)-[:INTERACTED_IN {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (c)-[:INTERACTED_IN {{date: {yesterday}}}]->(g)
        MERGE (c)-[:INTERACTED_IN {{date: {today}, localClusteringCoefficient: 1}}]->(g)
        MERGE (d)-[:INTERACTED_IN {{date: {yesterday}}}]->(g)
        MERGE (e)-[:INTERACTED_IN {{date: {today}, localClusteringCoefficient: 1}}]->(g)


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
    projection_utils = ProjectionUtils(guildId=guildId)
    computed_dates = projection_utils.get_computed_dates(
        """
        MATCH (:DiscordAccount)-[r:INTERACTED_IN]->(g:Guild {guildId: $guild_id})
        WHERE r.localClusteringCoefficient is NOT NULL
        RETURN r.date as computed_dates
        """,
        guild_id=guildId,
    )

    print(computed_dates)

    assert computed_dates == {today}
