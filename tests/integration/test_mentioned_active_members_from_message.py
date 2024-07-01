from datetime import datetime, timedelta

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


def test_mention_active_members_from_rawinfo():
    """
    test whether the people are being mentioned are active or not
    the shouldn't considered as active as we're not counting them
    the rawmemberactivities is used
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    acc_id = [
        "user1",
        "user2",
    ]
    analyzer = setup_platform(
        db_access=db_access,
        platform_id=platform_id,
        discordId_list=acc_id,
        days_ago_period=7,
    )

    db_access.db_mongo_client[platform_id].create_collection("heatmaps")
    db_access.db_mongo_client[platform_id].create_collection("memberactivities")

    # generating rawinfo samples
    rawinfo_samples = []

    # generating random rawinfo data
    # all user1 mentioning user2
    for i in range(150):
        author = "user1"
        mentioned_user = "user2"
        sample = [
            {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": author,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [
                    {
                        "name": "mention",
                        "type": "emitter",
                        "users_engaged_id": [mentioned_user],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "1020707129214111827",
                    "thread_id": None,
                },
                "source_id": f"11188143219343360{i}",
            },
            {
                "actions": [],
                "author_id": mentioned_user,
                "date": datetime.now() - timedelta(hours=i),
                "interactions": [
                    {
                        "name": "mention",
                        "type": "receiver",
                        "users_engaged_id": [author],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "1020707129214111827",
                    "thread_id": None,
                },
                "source_id": f"11188143219343360{i}",
            },
        ]
        rawinfo_samples.extend(sample)

    db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
        rawinfo_samples
    )

    analyzer.run_once()

    memberactivities_cursor = db_access.query_db_find(
        "memberactivities", {}, sorting=("date", -1)
    )
    memberactivities_data = list(memberactivities_cursor)

    print("memberactivities_data: ", memberactivities_data)

    # just user1 was mentioning others
    # user2 didn't do anything
    assert memberactivities_data[0]["all_active"] == ["user1"]
