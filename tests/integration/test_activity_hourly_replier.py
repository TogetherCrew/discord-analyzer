from datetime import datetime, timedelta

from discord_analyzer.analysis.activity_hourly import activity_hourly
from discord_analyzer.analyzer.analyzer_heatmaps import Heatmaps


def test_reply_messages():
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
                    "mess_type": 19,
                    "author": acc,
                    "user_mentions": [],
                    "reactions": [],
                    "replied_user": "876487027099582520",
                    "datetime": data_date,
                    "channel": chId,
                    "threadId": None,
                }

                prepared_list.append(prepared_data)
                channelIds.add(chId)
                dates.add(data_date.strftime("%Y-%m-%d"))

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
        assert document["reacted_per_acc"] == []
        assert document["mentioner_per_acc"] == []
        assert sum(document["lone_messages"]) == 0
        assert sum(document["thr_messages"]) == 0
        assert sum(document["mentioner"]) == 0
        assert sum(document["mentioned"]) == 0
        assert sum(document["reacter"]) == 0

        # if it is the account that everyone replied to
        if document["account_name"] == "876487027099582520":
            # the only document we have
            assert document["replied_per_acc"] == [
                # `len(acc_names) - 2` is because
                # len is returning one more and we are replying one account less
                ({"account": "876487027099582520", "count": len(acc_names) - 2},),
                ({"account": "876487027099582521", "count": len(acc_names) - 2},),
                ({"account": "876487027099582522", "count": len(acc_names) - 2},),
                ({"account": "876487027099582523", "count": len(acc_names) - 2},),
                ({"account": "876487027099582524", "count": len(acc_names) - 2},),
                ({"account": "876487027099582525", "count": len(acc_names) - 2},),
                ({"account": "876487027099582526", "count": len(acc_names) - 2},),
                ({"account": "876487027099582527", "count": len(acc_names) - 2},),
                ({"account": "876487027099582528", "count": len(acc_names) - 2},),
                ({"account": "876487027099582529", "count": len(acc_names) - 2},),
            ]
            assert sum(document["replier"]) == len(hours_to_include)
            assert sum(document["replied"]) == len(hours_to_include) * (
                len(acc_names) - 1
            )
        # other accounts
        else:
            assert sum(document["replier"]) == len(hours_to_include)
