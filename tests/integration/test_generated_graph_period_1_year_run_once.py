from datetime import datetime, timedelta, timezone

import numpy as np
from tc_neo4j_lib.neo4j_ops import Neo4jOps

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.mock_heatmaps import create_empty_heatmaps_data
from .utils.mock_memberactivities import create_empty_memberactivities_data
from .utils.remove_and_setup_guild import setup_db_guild


def test_networkgraph_one_year_period_run_once_available_analytics():
    """
    test the network graph for the whole analyzer pipeline
    of a guild with a 1 year period
    and use recompute method with some analytics data available
    """
    # first create the collections
    guildId = "1234"
    community_id = "aabbccddeeff001122334455"
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(guildId)
    neo4j_ops = Neo4jOps.get_instance()

    neo4j_ops.gds.run_cypher(
        """
        MATCH (n) DETACH DELETE (n)
        """
    )

    acc_id = [
        "973993299281076285",
        "973993299281076286",
    ]

    setup_db_guild(
        db_access,
        platform_id,
        guildId,
        discordId_list=acc_id,
        days_ago_period=360,
        community_id=community_id,
    )

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # filling memberactivities with some data
    # filling heatmaps with some data
    # filling up to 2 days ago with 353 documents
    memberactivity_data = create_empty_memberactivities_data(
        datetime.now() - timedelta(days=354), count=350
    )
    db_access.db_mongo_client[guildId]["memberactivities"].insert_many(
        memberactivity_data
    )

    # filling heatmaps with some data
    # filling up to 2 days ago with 359 documents
    # just yesterday is left to be analyzed
    heatmaps_data = create_empty_heatmaps_data(
        datetime.now() - timedelta(days=360), count=356
    )
    db_access.db_mongo_client[guildId]["heatmaps"].insert_many(heatmaps_data)

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # 24 hours
    # 360 days
    for i in range(24 * 360):
        sample = {
            "type": 19,
            "author": np.random.choice(acc_id),
            "content": f"test{i}",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": np.random.choice(acc_id),
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"11188143219343360{i}",
            "channelId": "1020707129214111827",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer(guildId)
    analyzer.run_once()

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH (g:Guild {{guildId: '{guildId}'}})-[r:HAVE_METRICS]-> (g)
        RETURN DISTINCT r.date as dates
        ORDER BY dates DESC
        """
    )
    dates = results.values.squeeze()

    print("dates[:2]: ", dates[:2])
    print("dates[-2:]: ", dates[-2:])

    # our analysis started from 4 days ago
    start_analytics_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    ) - timedelta(days=4)
    end_analytics_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    ) - timedelta(days=1)

    assert dates[-1] == start_analytics_date.timestamp() * 1000
    assert dates[0] == end_analytics_date.timestamp() * 1000

    results = neo4j_ops.gds.run_cypher(
        f"""
        MATCH
            (g:Guild {{guildId: '{guildId}'}})
                -[r:IS_WITHIN]-> (c:Community {{id: '{community_id}'}})
        RETURN c.id as cid
        """
    )
    assert len(results.values) == 1
    assert results["cid"].values == [community_id]
