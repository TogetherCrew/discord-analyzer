from discord_analyzer.algorithms.analytics_interactions_script import (
    filter_channel_thread,
)


def test_filter_channel_thread_single_empty_input():
    sample_input = []

    output = filter_channel_thread(sample_input)

    assert output == {}


def test_filter_channel_thread_multiple_empty_inputs():
    sample_input = []

    output = filter_channel_thread(
        sample_input,
    )

    assert output == {}


def test_filter_channel_thread_single_channel_single_message():
    sample_input = [
        {
            "author": "ahmadyazdanii#7517",
            "content": "test",
            "createdDate": "2023-04-19 07:05:17",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": None,
            "threadName": None,
        }
    ]

    output = filter_channel_thread(
        sample_input,
    )

    sample_output = {"off-topic": {None: {"1:ahmadyazdanii#7517": "test"}}}

    assert output == sample_output


# flake8: noqa
def test_filter_channel_thread_multiple_channel_multiple_message_single_user_all_channels():
    sample_input = [
        {
            "author": "ahmadyazdanii#7517",
            "content": "test",
            "createdDate": "2023-04-19 07:05:17",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": None,
            "threadName": None,
        },
        {
            "author": "ahmadyazdanii#7517",
            "content": "hi",
            "createdDate": "2023-04-19 07:05:18",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": "1098202658390691930",
            "threadName": "test",
        },
        {
            "author": "ahmadyazdanii#7517",
            "content": "test2",
            "createdDate": "2023-04-19 07:14:57",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": "1098202658390691930",
            "threadName": "test",
        },
    ]

    output = filter_channel_thread(
        sample_input,
    )

    sample_output = {
        "off-topic": {
            None: {"1:ahmadyazdanii#7517": "test"},
            "test": {
                "1:ahmadyazdanii#7517": "hi",
                "2:ahmadyazdanii#7517": "test2",
            },
        }
    }

    assert output == sample_output


def test_filter_channel_thread_single_channel_multiple_message_multiple_user_all_channels():  # flake8: noqa
    sample_input = [
        {
            "author": "ahmadyazdanii#7517",
            "content": "test",
            "createdDate": "2023-03-10 07:05:17",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": None,
            "threadName": None,
        },
        {
            "author": "Ene",
            "content": "Hello",
            "createdDate": "2023-03-11 07:05:17",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": "1098202658390691930",
            "threadName": "test-thread",
        },
        {
            "author": "Amin",
            "content": "Hi",
            "createdDate": "2023-03-12 07:05:18",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": "1098202658390691930",
            "threadName": "test-thread",
        },
        {
            "author": "Behzad",
            "content": "Ola!",
            "createdDate": "2023-04-07 07:14:57",
            "channelId": "993163081939165240",
            "channelName": "off-topic",
            "threadId": "1098202658390691930",
            "threadName": "test-thread",
        },
        {
            "author": "Nima",
            "content": "Salam!",
            "createdDate": "2023-04-12 07:14:57",
            "channelId": "993163081939165222",
            "channelName": "off-topic-2",
            "threadId": "1098202658390691931",
            "threadName": "test-thread2",
        },
    ]

    output = filter_channel_thread(
        sample_input,
    )

    sample_output = {
        "off-topic": {
            None: {"1:ahmadyazdanii#7517": "test"},
            "test-thread": {
                "1:Ene": "Hello",
                "2:Amin": "Hi",
                "3:Behzad": "Ola!",
            },
        },
        "off-topic-2": {"test-thread2": {"1:Nima": "Salam!"}},
    }

    assert output == sample_output
