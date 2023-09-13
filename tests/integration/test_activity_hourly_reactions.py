from datetime import datetime, timedelta

from discord_analyzer.analysis.activity_hourly import activity_hourly
from discord_analyzer.analyzer.analyzer_heatmaps import Heatmaps


def test_reacted_messages():
    # data preparation
    day = datetime(2023, 1, 1)
    # hours to include interactions
    hours_to_include = [2, 4, 5, 13, 16, 18, 19, 20, 21]
    DAY_COUNT = 3

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
                    "user_mentions": [],
                    "reactions": [
                        "876487027099582520,876487027099582521,👍",
                        "876487027099582522,heatface",
                    ],
                    "replied_user": None,
                    "datetime": data_date,
                    "channel": chId,
                    "threadId": None,
                }

                prepared_list.append(prepared_data)
                channelIds.add(chId)
                dates.add(data_date.strftime("%Y-%m-%d"))

    reacted_accs = set(
        ["876487027099582520", "876487027099582521", "876487027099582522"]
    )

    (_, heatmap_data) = activity_hourly(prepared_list, acc_names=acc_names)

    analyzer_heatmaps = Heatmaps("DB_connection", testing=False)
    results = analyzer_heatmaps._post_process_data(heatmap_data, len(acc_names))

    # print(results)

    assert len(results) == (len(acc_names) - 1) * DAY_COUNT
    for document in results:
        assert document["account_name"] in acc_names
        assert document["date"] in dates
        assert document["account_name"] in acc_names
        assert document["channelId"] in channelIds
        assert sum(document["thr_messages"]) == 0
        assert sum(document["mentioner"]) == 0
        assert sum(document["replied"]) == 0
        assert sum(document["replier"]) == 0
        assert sum(document["mentioned"]) == 0
        assert document["mentioner_per_acc"] == []
        assert document["replied_per_acc"] == []
        assert sum(document["lone_messages"]) == len(hours_to_include)

        if document["account_name"] not in reacted_accs:
            assert document["reacted_per_acc"] == [
                ({"account": "876487027099582520", "count": len(acc_names) - 2},),
                ({"account": "876487027099582521", "count": len(acc_names) - 2},),
                ({"account": "876487027099582522", "count": len(acc_names) - 2},),
            ]

            # the only document we have
            # 3 is the emoji count
            assert sum(document["reacter"]) == 0
            assert sum(document["reacted"]) == len(hours_to_include) * len(reacted_accs)
        else:
            assert sum(document["reacter"]) == len(hours_to_include) * (
                len(acc_names) - 2
            )
            assert sum(document["reacted"]) == len(hours_to_include) * (
                len(reacted_accs) - 1
            )
