from tc_analyzer_lib.automation.utils.automation_base import AutomationBase


def test_subtract_users_empty_data():
    """
    in case of no data for days ago
    """
    automation_base = AutomationBase()

    users1 = []
    users2 = []

    users = automation_base._subtract_users(users1, users2)

    assert users == set([])


def test_subtract_users_some_data_past_two_days():
    """
    in case of having some data for two days ago
    """
    automation_base = AutomationBase()

    users1 = []
    users2 = ["user2", "user3"]

    users = automation_base._subtract_users(users1, users2)

    assert users == set([])


def test_subtract_users_one_user_yesterday():
    """
    in case of having one users for yesterday
    """
    automation_base = AutomationBase()

    users1 = ["user2"]
    users2 = []

    users = automation_base._subtract_users(users1, users2)

    assert users == set(["user2"])


def test_subtract_users_multiple_users_yesterday():
    """
    in case of having multiple users for yesterday
    """
    automation_base = AutomationBase()

    users1 = ["user2", "user3", "user4"]
    users2 = []

    users = automation_base._subtract_users(users1, users2)

    assert users == set(["user2", "user3", "user4"])


def test_subtract_users_multiple_users_non_overlapping_both_days():
    """
    in case of having multiple users for
    both yesterday and two days ago but non overlapping users
    """
    automation_base = AutomationBase()

    users1 = ["user2", "user3", "user4"]
    users2 = ["user6", "user7"]

    users = automation_base._subtract_users(users1, users2)

    assert users == set(["user2", "user3", "user4"])


def test_subtract_users_multiple_users_overlapping_both_days():
    """
    in case of having multiple users for
    both yesterday and two days ago with overlapping users
    """
    automation_base = AutomationBase()

    users1 = ["user2", "user3", "user4"]
    users2 = ["user2", "user7"]

    users = automation_base._subtract_users(users1, users2)

    assert users == set(["user3", "user4"])
