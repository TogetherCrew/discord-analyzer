# test out local clustering coefficient with all nodes connected
from discord_analyzer.analysis.neo4j_analysis.local_clustering_coefficient import (
    LocalClusteringCoeff,
)

from tc_neo4j_lib.neo4j_ops import Neo4jOps


def test_all_connected_coeffs():
    """
    3 nodes all connected
    using two dates: 166 and 167
    in date 166 the coeffs are 1.0
    and in date 167 the coeffs are 0.0

    To see more info for this test:
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
        SET a.userId = "1000"
        SET b.userId = "1001"
        SET c.userId = "1002"
        MERGE (a) -[r:INTERACTED_WITH {{weight: 1, date: {yesterday}}}]->(b)
        MERGE (a) -[r2:INTERACTED_WITH {{weight: 2, date: {today}}}]->(b)
        MERGE (a) -[r3:INTERACTED_WITH {{weight: 3, date: {yesterday}}}]->(c)
        MERGE (b) -[r4:INTERACTED_WITH {{weight: 2, date: {yesterday}}}]->(c)
        SET r.guildId = '{guildId}'
        SET r2.guildId = '{guildId}'
        SET r3.guildId = '{guildId}'
        SET r4.guildId = '{guildId}'
        """
    )
    lcc = LocalClusteringCoeff(gds=neo4j_ops.gds)
    lcc.compute(guildId=guildId, from_start=True)

    # getting the results
    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (a:DiscordAccount) -[r:INTERACTED_IN]-> (:Guild {{guildId: '{guildId}'}})
        RETURN
            a.userId as userId,
            r.date as date,
            r.localClusteringCoefficient as lcc
        """
    )

    user0_id = "1000"
    expected_results_user0 = [
        [user0_id, yesterday, 1.0],
        [user0_id, today, 0.0],
    ]
    assert expected_results_user0 in results[results.userId == user0_id].values

    user1_id = "1001"
    expected_results_user1 = [
        [user1_id, yesterday, 1.0],
        [user1_id, today, 0.0],
    ]
    assert expected_results_user1 in results[results.userId == user1_id].values

    user2_id = "1002"
    expected_results_user2 = [
        [user2_id, yesterday, 1.0],
        [user2_id, today, 0.0],
    ]
    assert expected_results_user2 in results[results.userId == user2_id].values
