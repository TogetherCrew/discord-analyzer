from datetime import datetime, timedelta

from discord_analyzer.analyzer.memberactivity_utils import MemberActivityUtils

from .utils.analyzer_setup import launch_db_access, setup_analyzer
from .utils.remove_and_setup_guild import setup_db_guild


def test_utils_get_members():
    analyzer = setup_analyzer(guildId, platform_id)
    platform_id = "515151515151515151515151"
    guildId = "1012430565959553145"
    db_access = launch_db_access(guildId)
    users = ["973993299281076285"]

    setup_db_guild(db_access, platform_id, guildId, discordId_list=users)

    rawinfo_samples = []
    for i in range(150):
        sample = {
            "type": 0,
            "author": "973993299281076285",
            "content": "test10",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
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

    db_access.db_mongo_client[guildId].create_collection("heatmaps")
    db_access.db_mongo_client[guildId].create_collection("memberactivities")

    memberactivities_utils = MemberActivityUtils(analyzer.DB_connections)

    database_users = memberactivities_utils.get_all_users(guildId=guildId)

    print(f"database_users: {database_users}")
    assert database_users == users
