from datetime import datetime, timedelta

from tc_analyzer_lib.metrics.memberactivity_utils import MemberActivityUtils

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_utils_get_members():
    platform_id = "515151515151515151515151"
    users = ["user_0"]
    db_access = launch_db_access(platform_id)
    _ = setup_platform(db_access, platform_id, discordId_list=users, days_ago_period=7)

    rawinfo_samples = []
    for i in range(150):
        author = "user_0"
        sample = {
            "actions": [{"name": "message", "type": "emitter"}],
            "author_id": author,
            "date": datetime.now() - timedelta(hours=i),
            "interactions": [],
            "metadata": {
                "bot_activity": False,
                "channel_id": "1020707129214111827",
                "thread_id": None,
            },
            "source_id": f"11188143219343360{i}",
        }
        rawinfo_samples.append(sample)

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
        rawinfo_samples
    )

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    memberactivities_utils = MemberActivityUtils()

    database_users = memberactivities_utils.get_all_users(guildId=platform_id)

    print(f"database_users: {database_users}")
    assert database_users == users
