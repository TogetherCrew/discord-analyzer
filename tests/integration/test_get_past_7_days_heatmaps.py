from datetime import datetime, timedelta

import numpy as np
from tc_analyzer_lib.algorithms.utils.member_activity_utils import get_users_past_window

from .utils.analyzer_setup import launch_db_access


def test_get_past_7_days_heatmap_users_available_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

    db_access.db_mongo_client[platform_id].create_collection("heatmaps")

    heatmaps_data = []
    acc_names = []
    for i in range(250):
        date = start_date + timedelta(days=i)
        account = f"user{i}"
        document = {
            "date": date,
            "channel_id": "1020707129214111827",
            "thr_messages": list(np.zeros(24)),
            "lone_messages": list(np.zeros(24)),
            "replier": list(np.zeros(24)),
            "replied": list(np.zeros(24)),
            "mentioner": list(np.zeros(24)),
            "mentioned": list(np.zeros(24)),
            "reacter": list(np.zeros(24)),
            "reacted": list(np.zeros(24)),
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
            "user": account,
        }

        heatmaps_data.append(document)
        acc_names.append(account)

    db_access.db_mongo_client[platform_id]["heatmaps"].insert_many(heatmaps_data)

    start_date = datetime(2023, 1, 1) + timedelta(days=243)

    user_names = get_users_past_window(
        start_date,
        start_date + timedelta(days=250),
        db_access.db_mongo_client[platform_id]["heatmaps"],
    )

    assert set(user_names) == set(acc_names[-7:])


def test_get_all_days_heatmap_users_available_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

    db_access.db_mongo_client[platform_id].create_collection("heatmaps")

    heatmaps_data = []
    acc_names = []
    for i in range(250):
        date = start_date + timedelta(days=i)
        account = f"user{i}"
        document = {
            "date": date,
            "channel_id": "1020707129214111827",
            "thr_messages": list(np.zeros(24)),
            "lone_messages": list(np.zeros(24)),
            "replier": list(np.zeros(24)),
            "replied": list(np.zeros(24)),
            "mentioner": list(np.zeros(24)),
            "mentioned": list(np.zeros(24)),
            "reacter": list(np.zeros(24)),
            "reacted": list(np.zeros(24)),
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
            "user": account,
        }

        heatmaps_data.append(document)
        acc_names.append(account)

    db_access.db_mongo_client[platform_id]["heatmaps"].insert_many(heatmaps_data)

    user_names = get_users_past_window(
        window_start_date=datetime(2023, 1, 1),
        window_end_date=(start_date + timedelta(days=250)),
        collection=db_access.db_mongo_client[platform_id]["heatmaps"],
    )

    assert set(user_names) == set(acc_names)


def test_get_just_7_days_heatmap_users_available_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

    heatmaps_data = []
    acc_names = []
    for i in range(250):
        date = start_date + timedelta(days=i)
        account = f"user{i}"
        document = {
            "date": date,
            "channel_id": "1020707129214111827",
            "thr_messages": list(np.zeros(24)),
            "lone_messages": list(np.zeros(24)),
            "replier": list(np.zeros(24)),
            "replied": list(np.zeros(24)),
            "mentioner": list(np.zeros(24)),
            "mentioned": list(np.zeros(24)),
            "reacter": list(np.zeros(24)),
            "reacted": list(np.zeros(24)),
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
            "user": account,
        }

        heatmaps_data.append(document)
        acc_names.append(account)

    db_access.db_mongo_client[platform_id]["heatmaps"].insert_many(heatmaps_data)

    start_date = datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=7)

    user_names = get_users_past_window(
        start_date,
        end_date,
        db_access.db_mongo_client[platform_id]["heatmaps"],
    )
    print("user_names", user_names)

    assert set(user_names) == set(
        [
            "user0",
            "user1",
            "user2",
            "user3",
            "user4",
            "user5",
            "user6",
        ]
    )


def test_get_past_7_days_heatmap_users_no_users():
    """
    test if we're getting the right heatmap users
    """
    # first create the collections
    platform_id = "515151515151515151515151"
    db_access = launch_db_access(platform_id)

    start_date = datetime(2023, 1, 1)

    db_access.db_mongo_client[platform_id].drop_collection("heatmaps")

    db_access.db_mongo_client[platform_id].create_collection("heatmaps")

    start_date = datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=243)

    user_names = get_users_past_window(
        window_start_date=start_date,
        window_end_date=end_date,
        collection=db_access.db_mongo_client[platform_id]["heatmaps"],
    )

    assert user_names == []
