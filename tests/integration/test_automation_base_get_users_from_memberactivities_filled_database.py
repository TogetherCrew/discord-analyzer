from datetime import datetime, timedelta

from discord_analyzer.automation.utils.automation_base import AutomationBase

from .utils.analyzer_setup import launch_db_access


def test_automation_base_get_users_one_user_new_disengaged():
    """
    get empty users in case of no data available
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    date_yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    date_two_past_days = (datetime.now() - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
        [
            {
                "date": date_yesterday,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user1"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
            {
                "date": date_two_past_days,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": [],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
        ]
    )

    automation_base = AutomationBase()
    users1, users2 = automation_base._get_users_from_memberactivities(
        platform_id, category="all_new_disengaged"
    )

    assert users1 == ["user1"]
    assert users2 == []


def test_automation_base_get_users_multiple_user_new_disengaged():
    """
    get empty users in case of no data available
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

    date_yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    date_two_past_days = (datetime.now() - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
        [
            {
                "date": date_yesterday,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user1", "user2"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
            {
                "date": date_two_past_days,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user3", "user6", "user9"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": [],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
        ]
    )

    automation_base = AutomationBase()
    users1, users2 = automation_base._get_users_from_memberactivities(
        platform_id, category="all_new_disengaged"
    )

    assert users1 == ["user1", "user2"]
    assert users2 == ["user3", "user6", "user9"]


def test_automation_base_get_users_multiple_user_new_active():
    """
    get empty users in case of no data available
    """
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    db_access.db_mongo_client[platform_id].drop_collection("memberactivities")
    db_access.db_mongo_client[platform_id]["memberactivities"].delete_many({})

    date_yesterday = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    date_two_past_days = (datetime.now() - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    db_access.db_mongo_client[platform_id]["memberactivities"].insert_many(
        [
            {
                "date": date_yesterday,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": ["user10"],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user1", "user2"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": ["user5", "user8", "user12"],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
            {
                "date": date_two_past_days,
                "all_joined": [],
                "all_joined_day": [],
                "all_consistent": [],
                "all_vital": [],
                "all_active": [],
                "all_connected": [],
                "all_paused": [],
                "all_new_disengaged": ["user3", "user6", "user9"],
                "all_disengaged": [],
                "all_unpaused": [],
                "all_returned": [],
                "all_new_active": ["user10"],
                "all_still_active": [],
                "all_dropped": [],
                "all_disengaged_were_newly_active": [],
                "all_disengaged_were_consistently_active": [],
                "all_disengaged_were_vital": [],
                "all_lurker": [],
                "all_about_to_disengage": [],
                "all_disengaged_in_past": [],
            },
        ]
    )

    automation_base = AutomationBase()
    users1, users2 = automation_base._get_users_from_memberactivities(
        platform_id, category="all_new_active"
    )

    assert users1 == ["user5", "user8", "user12"]
    assert users2 == ["user10"]
