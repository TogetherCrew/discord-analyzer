from datetime import datetime, timedelta

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


def test_thread_messages():
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
    for i in range(3):
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
                chId = "channel_0"
                data_date = (day + timedelta(days=i)).replace(hour=hour)
                prepared_rawdata = {
                    "author_id": acc,
                    "date": data_date,
                    "source_id": f"9999{i}{hour}{acc}",  # message id it was
                    "actions": [{"name": "message", "type": "emitter"}],
                    "interactions": [],
                    "metadata": {
                        "channel_id": chId,
                        "thread_id": chId + "THREAD",
                    },
                }
                prepared_rawmemberactivities.append(prepared_rawdata)

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

    assert len(results) == len(acc_names) * DAY_COUNT
    for document in results:
        assert document["user"] in acc_names
        assert document["date"] in dates
        assert document["channel_id"] in channelIds
        assert document["reacted_per_acc"] == []
        assert document["mentioner_per_acc"] == []
        assert document["replied_per_acc"] == []
        assert sum(document["thr_messages"]) == len(hours_to_include)
        assert sum(document["mentioner"]) == 0
        assert sum(document["replied"]) == 0
        assert sum(document["replier"]) == 0
        assert sum(document["mentioned"]) == 0
        assert sum(document["reacter"]) == 0
        assert sum(document["lone_messages"]) == 0
