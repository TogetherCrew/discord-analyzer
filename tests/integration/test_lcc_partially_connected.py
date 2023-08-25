# the nodes of the graph are partially connected
# fmt: off
from discord_analyzer.analysis.neo4j_analysis.local_clustering_coefficient import \
    LocalClusteringCoeff

from .utils.neo4j_conn import neo4j_setup

# fmt: on


def test_partially_connected_coeffs():
    """
    5 nodes partially connected
    using two dates: 166 and 167

    To see more info for this test:
    https://miro.com/app/board/uXjVM7GdYqo=/?share_link_id=105382864070
    """
    neo4j_utils = neo4j_setup()
    # deleting all data
    neo4j_utils.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    # timestamps
    today = 1689280200.0
    yesterday = 1689193800.0
    guildId = "1234"

    # creating some nodes with data
    neo4j_utils.gds.run_cypher(
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
    lcc = LocalClusteringCoeff(gds=neo4j_utils.gds)
    lcc.compute(guildId=guildId)

    # getting the results
    results = neo4j_utils.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount) -[r:INTERACTED_IN]-> (:Guild {{guildId: '{guildId}'}})
        RETURN 
            a.userId as userId,
            r.date as date, 
            r.localClusteringCoefficient as lcc
        """
    )
    print(results.values)

    user0_id = "1000"
    expected_results_user0 = [
        [user0_id, yesterday, 1.0],
        [user0_id, today, 1.0],
    ]
    assert expected_results_user0 in results[results.userId == user0_id].values

    user1_id = "1001"
    expected_results_user1 = [
        [user1_id, yesterday, 2 / 3],
        [user1_id, today, 1 / 3],
    ]
    assert expected_results_user1 in results[results.userId == user1_id].values

    user2_id = "1002"
    expected_results_user2 = [
        [user2_id, yesterday, 1],
        [user2_id, today, 2 / 3],
    ]
    assert expected_results_user2 in results[results.userId == user2_id].values

    user3_id = "1003"
    expected_results_user3 = [
        [user3_id, yesterday, 2 / 3],
        [user3_id, today, 1],
    ]
    assert expected_results_user3 in results[results.userId == user3_id].values

    user4_id = "1003"
    expected_results_user4 = [
        [user4_id, yesterday, 2 / 3],
        [user4_id, today, 1],
    ]
    assert expected_results_user4 in results[results.userId == user4_id].values
