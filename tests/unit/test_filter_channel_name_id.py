from discord_analyzer.analysis.analytics_interactions_script import (
    filter_channel_name_id,
)


def test_filter_channel_name_empty_input():
    sample_input = []

    output = filter_channel_name_id(sample_input)

    assert output == {}


def test_filter_channel_name_one_synthesized_input():
    sample_input = [
        {
            "channelId": "123",
            "channelName": "welcome-and-rules",
        }
    ]

    output = filter_channel_name_id(sample_input)

    assert output == {"123": "welcome-and-rules"}


def test_filter_channel_name_multiple_synthesized_input():
    sample_input = [
        {
            "channelId": "123",
            "channelName": "welcome-and-rules",
        },
        {
            "channelId": "1234",
            "channelName": "welcome-and-rules2",
        },
        {
            "channelId": "12345",
            "channelName": "welcome-and-rules3",
        },
    ]

    output = filter_channel_name_id(sample_input)

    assert output == {
        "123": "welcome-and-rules",
        "1234": "welcome-and-rules2",
        "12345": "welcome-and-rules3",
    }


def test_filter_channel_name_one_real_input():
    sample_input = [
        {
            "_id": {"$oid": "6436d6ab47ce0ae8b83f25fc"},
            "channelId": "993163081939165236",
            "__v": 0,
            "channelName": "welcome-and-rules",
            "last_update": {"$date": "2023-05-10T01:00:05.379Z"},
        }
    ]

    output = filter_channel_name_id(sample_input)

    assert output == {"993163081939165236": "welcome-and-rules"}


def test_filter_channel_name_multiple_real_input():
    sample_input = [
        {
            "_id": {"$oid": "6436d6ab47ce0ae8b83f25fc"},
            "channelId": "993163081939165236",
            "__v": 0,
            "channelName": "welcome-and-rules",
            "last_update": {"$date": "2023-05-10T01:00:05.379Z"},
        },
        {
            "_id": {"$oid": "6436d6ab47ce0ae8b83f2600"},
            "channelId": "993163081939165237",
            "__v": 0,
            "channelName": "announcements",
            "last_update": {"$date": "2023-05-10T01:00:05.382Z"},
        },
        {
            "_id": {"$oid": "6436d6ab47ce0ae8b83f260a"},
            "channelId": "993163081939165238",
            "__v": 0,
            "channelName": "resources",
            "last_update": {"$date": "2023-05-10T01:00:05.385Z"},
        },
        {
            "_id": {"$oid": "6436d6ab47ce0ae8b83f2613"},
            "channelId": "993163081939165240",
            "__v": 0,
            "channelName": "general",
            "last_update": {"$date": "2023-05-10T01:00:05.407Z"},
        },
    ]

    output = filter_channel_name_id(sample_input)

    assert output == {
        "993163081939165236": "welcome-and-rules",
        "993163081939165237": "announcements",
        "993163081939165238": "resources",
        "993163081939165240": "general",
    }
