from datetime import datetime, timedelta

import numpy as np


def create_empty_heatmaps_data(
    start_date: datetime, count: int = 10
) -> list[dict[str, list]]:
    """
    create empty documents of heatmaps
    """
    data = []
    for i in range(count):
        date = start_date + timedelta(days=i)
        document = {
            "date": date.strftime("%Y-%m-%d"),
            "channelId": "1020707129214111827",
            "thr_messages": list(np.zeros(24)),
            "lone_messages": list(np.zeros(24)),
            "replier": list(np.zeros(24)),
            "replied": list(np.zeros(24)),
            "mentioner": list(np.zeros(24)),
            "mentioned": list(np.zeros(24)),
            "reacter": list(np.zeros(24)),
            "reacted": list(np.zeros(24)),
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
            "account_name": "973993299281076285",
        }
        data.append(document)

    return data
