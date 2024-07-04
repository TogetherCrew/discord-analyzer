from datetime import datetime, timedelta

from tc_analyzer_lib.algorithms.utils.member_activity_history_utils import (
    MemberActivityPastUtils,
)


def test_zero_joined():
    db_access = None

    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2023, 4, 15)

    all_joined_day = {}
    joined_acc = [
        {"joined_at": (start_dt + timedelta(days=5)), "id": "000000000"},
        {"joined_at": (start_dt + timedelta(days=6)), "id": "000000001"},
        {"joined_at": (start_dt + timedelta(days=8)), "id": "000000002"},
    ]

    member_activitiy_utils = MemberActivityPastUtils(db_access=db_access)
    starting_key = 0

    all_joined_day = member_activitiy_utils.update_all_joined_day(
        start_dt=start_dt,
        end_dt=end_dt,
        all_joined_day=all_joined_day,
        starting_key=starting_key,
        joined_acc=joined_acc,
    )

    assert all_joined_day["0"] == set([])
    assert all_joined_day["1"] == set([])
    assert all_joined_day["2"] == set([])
    assert all_joined_day["3"] == set([])
    assert all_joined_day["4"] == set([])
    assert all_joined_day["5"] == set(["000000000"])
    assert all_joined_day["6"] == set(["000000001"])
    assert all_joined_day["7"] == set([])
    assert all_joined_day["8"] == set(["000000002"])
    for i in range(9, (end_dt - start_dt).days):
        assert all_joined_day[str(i)] == set([])

    # len would show 1 more
    assert len(all_joined_day.keys()) - 1 == (end_dt - start_dt).days + starting_key


def test_single_joined():
    db_access = None

    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2023, 4, 15)

    all_joined_day = {
        "0": set(["000000000", "000000001"]),
    }

    joined_acc = [
        {"joined_at": (start_dt + timedelta(days=0)), "id": "000000002"},
        {"joined_at": (start_dt + timedelta(days=1)), "id": "000000003"},
        {"joined_at": (start_dt + timedelta(days=2)), "id": "000000004"},
    ]

    member_activitiy_utils = MemberActivityPastUtils(db_access=db_access)
    starting_key = 1

    all_joined_day = member_activitiy_utils.update_all_joined_day(
        start_dt=start_dt,
        end_dt=end_dt,
        all_joined_day=all_joined_day,
        starting_key=starting_key,
        joined_acc=joined_acc,
    )

    assert all_joined_day["0"] == set(["000000000", "000000001"])
    assert all_joined_day["1"] == set(["000000002"])
    assert all_joined_day["2"] == set(["000000003"])
    assert all_joined_day["3"] == set(["000000004"])
    for i in range(4, (end_dt - start_dt).days):
        assert all_joined_day[str(i)] == set([])

    # len would show 1 more
    assert len(all_joined_day.keys()) - 1 == (end_dt - start_dt).days + starting_key


def test_multiple_joined():
    """Test multiple accounts joined in a day"""
    db_access = None

    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2023, 4, 15)

    all_joined_day = {
        "0": set(["000000000", "000000001"]),
        "1": set(["000000002", "000000003"]),
    }

    joined_acc = [
        {"joined_at": (start_dt + timedelta(days=0)), "id": "000000004"},
        {"joined_at": (start_dt + timedelta(days=0)), "id": "000000005"},
        {"joined_at": (start_dt + timedelta(days=2)), "id": "000000006"},
        {"joined_at": (start_dt + timedelta(days=2)), "id": "000000007"},
        {"joined_at": (start_dt + timedelta(days=2)), "id": "000000008"},
    ]

    member_activitiy_utils = MemberActivityPastUtils(db_access=db_access)
    starting_key = 2

    all_joined_day = member_activitiy_utils.update_all_joined_day(
        start_dt=start_dt,
        end_dt=end_dt,
        all_joined_day=all_joined_day,
        starting_key=starting_key,
        joined_acc=joined_acc,
    )

    assert all_joined_day["0"] == set(["000000000", "000000001"])
    assert all_joined_day["1"] == set(["000000002", "000000003"])
    assert all_joined_day["2"] == set(["000000004", "000000005"])
    assert all_joined_day["3"] == set([])
    assert all_joined_day["4"] == set(["000000006", "000000007", "000000008"])

    for i in range(5, (end_dt - start_dt).days):
        assert all_joined_day[str(i)] == set([])

    # len would show 1 more
    assert len(all_joined_day.keys()) - 1 == (end_dt - start_dt).days + starting_key
