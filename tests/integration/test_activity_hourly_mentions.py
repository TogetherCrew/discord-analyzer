from datetime import datetime, timedelta

from discord_analyzer.analysis.activity_hourly import activity_hourly
from discord_analyzer.analyzer.analyzer_heatmaps_old import Heatmaps


def test_mentioned_messages():
    # data preparation
    day = datetime(2023, 1, 1)
    # hours to include interactions
    hours_to_include = [2, 4, 5, 13, 16, 18, 19, 20, 21]
    DAY_COUNT = 2

    acc_names = []
    for i in range(10):
        acc_names.append(f"87648702709958252{i}")

    prepared_list = []
    channelIds = set()
    dates = set()

    for i in range(DAY_COUNT):
        for hour in hours_to_include:
            for acc in acc_names:
                data_date = (day + timedelta(days=i)).replace(hour=hour)
                chId = f"10207071292141118{i}"
                prepared_data = {
                    "mess_type": 0,
                    "author": acc,
                    "user_mentions": ["876487027099582520", "876487027099582521"],
                    "reactions": [],
                    "replied_user": None,
                    "datetime": data_date,
                    "channel": chId,
                    "threadId": None,
                }

                prepared_list.append(prepared_data)
                channelIds.add(chId)
                dates.add(data_date.strftime("%Y-%m-%d"))

    accs_mentioned = ["876487027099582520", "876487027099582521"]

    (_, heatmap_data) = activity_hourly(prepared_list, acc_names=acc_names)

    analyzer_heatmaps = Heatmaps("DB_connection", testing=False)
    results = analyzer_heatmaps._post_process_data(heatmap_data, len(acc_names))

    assert len(results) == (len(acc_names) - 1) * DAY_COUNT
    for document in results:
        assert document["account_name"] in acc_names
        assert document["date"] in dates
        assert document["channelId"] in channelIds
        assert document["reacted_per_acc"] == []
        assert sum(document["thr_messages"]) == 0
        assert sum(document["reacter"]) == 0
        assert sum(document["replied"]) == 0
        assert sum(document["replier"]) == 0
        assert document["replied_per_acc"] == []
        assert sum(document["lone_messages"]) == len(hours_to_include)

        if document["account_name"] == "876487027099582520":
            assert document["mentioner_per_acc"] == [
                ({"account": "876487027099582521", "count": (len(acc_names) - 2)},)
            ]
            assert sum(document["mentioner"]) == len(hours_to_include)
            assert sum(document["mentioned"]) == len(hours_to_include) * (
                len(acc_names) - 2
            )

        elif document["account_name"] == "876487027099582521":
            assert document["mentioner_per_acc"] == [
                ({"account": "876487027099582520", "count": (len(acc_names) - 2)},)
            ]
            assert sum(document["mentioner"]) == len(hours_to_include)
            assert sum(document["mentioned"]) == len(hours_to_include) * (
                len(acc_names) - 2
            )
        else:
            assert document["mentioner_per_acc"] == [
                ({"account": "876487027099582520", "count": 9},),
                ({"account": "876487027099582521", "count": 9},),
            ]
            assert sum(document["mentioner"]) == len(hours_to_include) * len(
                accs_mentioned
            )
            assert sum(document["mentioned"]) == 0
