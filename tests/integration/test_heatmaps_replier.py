from datetime import datetime, timedelta

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


def test_reply_messages():
    platform_id = "1122334455"
    mongo_client = MongoSingleton.get_instance().get_client()
    database = mongo_client[platform_id]

    database.drop_collection("rawmemberactivities")
    database.drop_collection("rawmembers")
    # data preparation
    DAY_COUNT = 3
    day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=DAY_COUNT
    )
    # hours to include interactions
    hours_to_include = [2, 4, 5, 13, 16, 18, 19, 20, 21]

    acc_names = []
    prepared_rawmembers = []
    for i in range(5):
        acc = f"user_{i}"
        acc_names.append(acc)

        prepared_member = {
            "id": acc,
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, i + 1, 1),
            "options": {},
        }
        prepared_rawmembers.append(prepared_member)

    prepared_rawmemberactivities = []
    channelIds = set()
    dates = set()

    for i in range(DAY_COUNT):
        for hour in hours_to_include:
            for acc in acc_names:
                data_date = (day + timedelta(days=i)).replace(hour=hour)
                chId = "channel_0"
                source_id = f"9999{i}{hour}{acc}"  # message id it was
                prepared_rawdata = [
                    {
                        "author_id": acc,
                        "date": data_date,
                        "source_id": source_id,
                        "actions": [{"name": "message", "type": "emitter"}],
                        "interactions": [
                            {
                                "name": "reply",
                                "users_engaged_id": ["user_1"],
                                "type": "emitter",
                            }
                        ],
                        "metadata": {
                            "channel_id": chId,
                            "thread_id": None,
                        },
                    },
                    {
                        "author_id": "user_1",
                        "date": data_date,
                        "source_id": source_id,
                        "actions": [],
                        "interactions": [
                            {
                                "name": "reply",
                                "users_engaged_id": [acc],
                                "type": "receiver",
                            }
                        ],
                        "metadata": {
                            "channel_id": chId,
                            "thread_id": None,
                        },
                    },
                ]
                prepared_rawmemberactivities.extend(prepared_rawdata)

                channelIds.add(chId)
                dates.add(data_date.replace(hour=0, minute=0, second=0, microsecond=0))

    database["rawmemberactivities"].insert_many(prepared_rawmemberactivities)
    database["rawmembers"].insert_many(prepared_rawmembers)

    analyzer_heatmaps = Heatmaps(
        platform_id=platform_id,
        period=day,
        resources=list(channelIds),
        analyzer_config=DiscordAnalyzerConfig(),
    )
    results = analyzer_heatmaps.start(from_start=True)

    assert len(results) == len(acc_names) * DAY_COUNT * len(channelIds)
    for document in results:
        assert document["user"] in acc_names
        assert document["date"] in dates
        assert document["user"] in acc_names
        assert document["channel_id"] in channelIds
        assert document["reacted_per_acc"] == []
        assert document["mentioner_per_acc"] == []
        # the message action
        assert sum(document["lone_messages"]) == len(hours_to_include)
        assert sum(document["thr_messages"]) == 0
        assert sum(document["mentioner"]) == 0
        assert sum(document["mentioned"]) == 0
        assert sum(document["reacter"]) == 0

        if document["user"] == "user_1":
            assert document["replied_per_acc"] == []
            assert sum(document["replied"]) == 0
            assert sum(document["replier"]) == len(hours_to_include) * (
                len(acc_names) - 1
            )
        else:
            assert document["replied_per_acc"] == [
                {"account": "user_1", "count": len(hours_to_include)}
            ]
            assert sum(document["replied"]) == len(hours_to_include)
            assert sum(document["replier"]) == 0
