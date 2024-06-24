from discord_analyzer.analysis.neo4j_analysis.louvain import Louvain
from discord_analyzer.analysis.neo4j_analysis.utils import ProjectionUtils
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
    louvain = Louvain()
    projection_utils = ProjectionUtils(guildId=guild_id)

    computed_dates = louvain.get_computed_dates(projection_utils, guildId=guild_id)

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
        MERGE (g)-[:HAVE_METRICS {{date: {yesterday}}}]->(g)
        SET r.guildId = '{guild_id}'
        SET r2.guildId = '{guild_id}'
        SET r3.guildId = '{guild_id}'
        SET r4.guildId = '{guild_id}'
        """
    )
    louvain = Louvain()
    projection_utils = ProjectionUtils(guildId=guild_id)

    computed_dates = louvain.get_computed_dates(projection_utils, guildId=guild_id)

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
        MERGE (g)-[:HAVE_METRICS {{date: {yesterday}, louvainModularityScore: 0.0}}]->(g)
        SET r.guildId = '{guild_id}'
        SET r2.guildId = '{guild_id}'
        SET r3.guildId = '{guild_id}'
        SET r4.guildId = '{guild_id}'
        """
    )
    louvain = Louvain()
    projection_utils = ProjectionUtils(guildId=guild_id)

    computed_dates = louvain.get_computed_dates(projection_utils, guildId=guild_id)

    assert computed_dates == {yesterday}
    # clean-up
    neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")
