from datetime import datetime, timedelta

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


def test_reacted_messages():
    platform_id = "1122334455"
    mongo_client = MongoSingleton.get_instance().get_client()

    database = mongo_client[platform_id]

    database.drop_collection("rawmemberactivities")
    database.drop_collection("rawmembers")

    # data preparation
    DAY_COUNT = 2
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
            for author in acc_names:
                data_date = (day + timedelta(days=i)).replace(hour=hour)
                chId = "channel_0"
                source_id = f"9999{i}{hour}{author}"  # message id it was

                prepared_rawdata = [
                    {
                        "author_id": author,
                        "date": data_date,
                        "source_id": source_id,
                        "actions": [{"name": "message", "type": "emitter"}],
                        "interactions": [
                            {
                                "name": "reaction",
                                "users_engaged_id": ["user_0", "user_1", "user_2"],
                                "type": "receiver",
                            }
                        ],
                        "metadata": {
                            "channel_id": chId,
                            "thread_id": None,
                        },
                    },
                    {
                        "author_id": "user_0",
                        "date": data_date,
                        "source_id": source_id,
                        "actions": [],
                        "interactions": [
                            {
                                "name": "reaction",
                                "users_engaged_id": [author],
                                "type": "emitter",
                            },
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
                                "name": "reaction",
                                "users_engaged_id": [author],
                                "type": "emitter",
                            },
                        ],
                        "metadata": {
                            "channel_id": chId,
                            "thread_id": None,
                        },
                    },
                    {
                        "author_id": "user_2",
                        "date": data_date,
                        "source_id": source_id,
                        "actions": [],
                        "interactions": [
                            {
                                "name": "reaction",
                                "users_engaged_id": [author],
                                "type": "emitter",
                            },
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

    reacted_accs = set(["user_0", "user_1", "user_2"])

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
        assert sum(document["thr_messages"]) == 0
        assert sum(document["mentioner"]) == 0
        assert sum(document["replied"]) == 0
        assert sum(document["replier"]) == 0
        assert sum(document["mentioned"]) == 0
        assert document["mentioner_per_acc"] == []
        assert document["replied_per_acc"] == []
        assert sum(document["lone_messages"]) == len(hours_to_include)

        if document["user"] not in reacted_accs:
            assert document["reacted_per_acc"] == []

            # the only document we have
            # 3 is the emoji count
            assert sum(document["reacter"]) == len(hours_to_include) * len(reacted_accs)
            assert sum(document["reacted"]) == 0
        else:
            user = document["user"]

            for acc in set(acc_names) - set([user]):
                expected_raw_analytics_item = {
                    "account": acc,
                    "count": len(hours_to_include),
                }
                assert expected_raw_analytics_item in document["reacted_per_acc"]

            # the minus operations on acc_names
            # is for ignoring the self interaction
            assert sum(document["reacter"]) == len(hours_to_include) * (
                len(acc_names) - 2 - 1
            )
            assert sum(document["reacted"]) == len(hours_to_include) * (
                len(acc_names) - 1
            )
