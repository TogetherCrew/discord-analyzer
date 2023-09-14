from engagement_notifier.messages import get_disengaged_message


def test_disengagement_message_not_empty():
    """
    given the name, return the message with it
    """
    name = "Amin"
    message = get_disengaged_message(name)

    assert message is not None
    assert message != ""


def test_disengagement_message():
    """
    given the name, return the message with it
    """
    name = "Amin"
    message = get_disengaged_message(name)

    assert name in message
