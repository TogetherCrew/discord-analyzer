from datetime import datetime, timedelta
from typing import Any


def create_empty_memberactivities_data(
    start_date: datetime, count: int = 10
) -> list[dict[str, Any]]:
    """
    create empty documents of memberactivities
    """
    data: list[dict[str, Any]] = []

    for i in range(count):
        date = start_date + timedelta(days=i)
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        document = {
            "date": date,
            "all_joined": [],
            "all_joined_day": [],
            "all_consistent": [],
            "all_vital": [],
            "all_active": [],
            "all_connected": [],
            "all_paused": [],
            "all_new_disengaged": [],
            "all_disengaged": [],
            "all_unpaused": [],
            "all_returned": [],
            "all_new_active": [],
            "all_still_active": [],
            "all_dropped": [],
            "all_disengaged_were_newly_active": [],
            "all_disengaged_were_consistently_active": [],
            "all_disengaged_were_vital": [],
            "all_lurker": [],
            "all_about_to_disengage": [],
            "all_disengaged_in_past": [],
        }

        data.append(document)

    return data
