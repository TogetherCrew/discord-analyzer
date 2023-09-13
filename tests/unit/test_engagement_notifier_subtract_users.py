from engagement_notifier.engagement import EngagementNotifier


def test_subtract_users_empty_data():
    """
    in case of no data for days ago
    """
    notifier = EngagementNotifier()

    users1 = []
    users2 = []

    users = notifier._subtract_users(users1, users2)

    assert users == set([])


def test_subtract_users_some_data_past_two_days():
    """
    in case of having some data for two days ago
    """
    notifier = EngagementNotifier()

    users1 = []
    users2 = ["user2", "user3"]

    users = notifier._subtract_users(users1, users2)

    assert users == set([])


def test_subtract_users_one_user_yesterday():
    """
    in case of having one users for yesterday
    """
    notifier = EngagementNotifier()

    users1 = ["user2"]
    users2 = []

    users = notifier._subtract_users(users1, users2)

    assert users == set(["user2"])


def test_subtract_users_multiple_users_yesterday():
    """
    in case of having multiple users for yesterday
    """
    notifier = EngagementNotifier()

    users1 = ["user2", "user3", "user4"]
    users2 = []

    users = notifier._subtract_users(users1, users2)

    assert users == set(["user2", "user3", "user4"])


def test_subtract_users_multiple_users_non_overlapping_both_days():
    """
    in case of having multiple users for
    both yesterday and two days ago but non overlapping users
    """
    notifier = EngagementNotifier()

    users1 = ["user2", "user3", "user4"]
    users2 = ["user6", "user7"]

    users = notifier._subtract_users(users1, users2)

    assert users == set(["user2", "user3", "user4"])


def test_subtract_users_multiple_users_overlapping_both_days():
    """
    in case of having multiple users for
    both yesterday and two days ago with overlapping users
    """
    notifier = EngagementNotifier()

    users1 = ["user2", "user3", "user4"]
    users2 = ["user2", "user7"]

    users = notifier._subtract_users(users1, users2)

    assert users == set(["user3", "user4"])
