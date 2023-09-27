from engagement_notifier.messages import get_report_message


def test_report_message_empty_input():
    """
    test the module with empty input
    """
    message = get_report_message([])

    assert message == "The following members disengaged and were messaged:\n"


def test_report_message_single_user():
    """
    test the report message if a single user was given
    """
    message = get_report_message(["user1"])

    expected_message = "The following members disengaged and were messaged:\n"
    expected_message += "- user1\n"

    assert message == expected_message


def test_report_message_multiple_users():
    """
    test the report message if multiple user was given
    """
    message = get_report_message(["user1", "user2"])

    expected_message = "The following members disengaged and were messaged:\n"
    expected_message += "- user1\n"
    expected_message += "- user2\n"

    assert message == expected_message
