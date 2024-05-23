# test out local clustering coefficient with all nodes connected
from discord_analyzer.analysis.neo4j_analysis.analyzer_node_stats import NodeStats

from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_node_stats():
    """
    5 nodes partially connected
    using two dates: 166 and 167

    To see the graph for this test:
    https://miro.com/app/board/uXjVM7GdYqo=/?share_link_id=105382864070
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

    node_stats = NodeStats(threshold=2)
    node_stats.compute_stats(guildId="1234", from_start=True)

    # getting the results
    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount)
             -[r:INTERACTED_IN] -> (g:Guild {{guildId: '{guildId}'}})
        RETURN a.userId as userId, r.date as date, r.status as status
        """
    )

    # we had 5 discord accounts and 2 dates for each
    # just the "1004" user did not interact yesterday
    # so 9 status results
    assert len(results) == 9
    print(results)

    results_user0 = results[results["userId"] == "1000"]
    expected_results = [
        ["1000", today, 2],
        ["1000", yesterday, 0],
    ]
    assert expected_results in results_user0.values

    results_user1 = results[results["userId"] == "1001"]
    expected_results = [
        ["1001", today, 1],
        ["1001", yesterday, 1],
    ]
    assert expected_results in results_user1.values

    results_user2 = results[results["userId"] == "1002"]
    expected_results = [
        ["1002", today, 0],
        ["1002", yesterday, 0],
    ]
    assert expected_results in results_user2.values

    results_user3 = results[results["userId"] == "1003"]
    expected_results = [
        ["1003", today, 2],
        ["1004", yesterday, 1],
    ]
    assert expected_results in results_user3.values

    results_user4 = results[results["userId"] == "1004"]
    expected_results = [["1004", today, 0]]
    assert expected_results in results_user4.values
