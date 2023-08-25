from datetime import datetime, timedelta

import pytest

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


@pytest.mark.skip("No Mongo instance on GitHub actions!")
def test_mention_active_members_from_rawinfo():
    """
    test whether the people are being mentioned are active or not
    the shouldn't considered as active as we're not counting them
    the rawinfos is used
    """
    # first create the collections
    guildId = "1234"
    db_access = launch_db_access(guildId)

    acc_id = [
        "user1",
        "user2",
    ]
    setup_db_guild(db_access, guildId, discordId_list=acc_id, days_ago_period=7)

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # all user1 mentioning user2
    for i in range(150):
        sample = {
            "type": 0,
            "author": "user1",
            "content": f"test{i}",
            "user_mentions": ["user2"],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": (datetime.now() - timedelta(hours=i)),
            "messageId": f"11188143219343360{i}",
            "channelId": "1020707129214111827",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[guildId]["rawinfos"].insert_many(rawinfo_samples)

    analyzer = setup_analyzer()
    analyzer.run_once(guildId=guildId)

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities", {}, sorting=("date", -1)
    )
    memberactivities_data = list(memberactivities_cursor)

    print("memberactivities_data: ", memberactivities_data)

    # just user1 was mentioning others
    # user2 didn't do anything
    assert memberactivities_data[0]["all_active"] == ["user1"]
