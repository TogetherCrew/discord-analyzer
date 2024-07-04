from datetime import datetime, timedelta

from tc_analyzer_lib.models.RawInfoModel import RawInfoModel
from tc_analyzer_lib.utils.mongo import MongoSingleton


def test_rawinfo_get_day_entry_empty_data():
    """
    test rawinfo dailty data fetching with no data avaialbe
    """
    guildId = "1234"

    mongo_singleton = MongoSingleton.get_instance()
    client = mongo_singleton.get_client()

    client[guildId].drop_collection("rawmemberactivities")

    rawinfo_model = RawInfoModel(client[guildId])

    today = datetime.now()
    data = rawinfo_model.get_day_entries(today)

    assert data == []


def test_rawinfo_get_day_entry_data_avaialble():
    """
    test the rawinfo daily data fetching in case of some data available
    """
    guildId = "1234"

    mongo_singleton = MongoSingleton.get_instance()
    client = mongo_singleton.get_client()

    client[guildId].drop_collection("rawmemberactivities")

    specific_midday = datetime(2023, 3, 3, 12)

    # generating rawinfo samples
    rawinfo_samples = [
        {
            "type": 19,
            "author": "user1",
            "content": "test_message",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "user3",
            "createdDate": (specific_midday - timedelta(hours=1)),
            "messageId": "222222",
            "channelId": "1115555666777889",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        },
        {
            "type": 19,
            "author": "This is a test!",
            "content": "test_message",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "user3",
            "createdDate": (specific_midday - timedelta(hours=2)),
            "messageId": "222223",
            "channelId": "1115555666777889",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": True,
        },
        {
            "type": 19,
            "author": "This is a test!",
            "content": "test_message",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "user3",
            "createdDate": (specific_midday - timedelta(hours=3)),
            "messageId": "222224",
            "channelId": "1115555666777889",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": True,
        },
        {
            "type": 19,
            "author": "Hello",
            "content": "test_message",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "user3",
            "createdDate": (specific_midday - timedelta(hours=4)),
            "messageId": "222225",
            "channelId": "1115555666777889",
            "channelName": "general",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        },
    ]

    client[guildId]["rawmemberactivities"].insert_many(rawinfo_samples)

    rawinfo_model = RawInfoModel(client[guildId])

    data = rawinfo_model.get_day_entries(specific_midday)

    assert len(data) == 2
