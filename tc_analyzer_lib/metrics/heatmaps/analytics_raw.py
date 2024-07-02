import logging
from datetime import date, datetime, time, timedelta
from typing import Any

from tc_analyzer_lib.schemas import RawAnalyticsItem
from tc_analyzer_lib.utils.mongo import MongoSingleton


class AnalyticsRaw:
    def __init__(self, platform_id: str) -> None:
        client = MongoSingleton.get_instance().get_client()
        # `rawmemberactivities` is the collection we would use for analytics
        self.collection = client[platform_id]["rawmemberactivities"]
        self.msg_prefix = f"PLATFORMID: {platform_id}:"

    def analyze(
        self,
        day: date,
        activity: str,
        activity_name: str,
        activity_direction: str,
        author_id: int,
        **kwargs,
    ) -> list[RawAnalyticsItem]:
        """
        analyze the count of messages

        Parameters
        ------------
        day : datetime.date
            analyze for a specific day
        activity : str
            the activity to be `actions` or `interactions`
        activity_name : str
            the activity name to be used from `rawmemberactivities` data
            could be `reply`, `mention`, `message`, `commit` or any other
            thing that is available on `rawmemberactivities` data
        author_id : str
            the author to filter data for
        activity_direction : str
            should be always either `emitter` or `receiver`
        **kwargs :
            additional_filters : dict[str, str]
                the additional filtering for `rawmemberactivities` data of each platform
                the keys could be `metadata.channel_id` with a specific value

        Returns
        ---------
        activity_count : RawAnalyticsItem
            raw analytics item which holds the user and
            the count of interaction in that day
        """
        additional_filters: dict[str, str] = kwargs.get("additional_filters", {})

        if activity_direction not in ["emitter", "receiver"]:
            raise ValueError(
                "Wrong activity_direction given, "
                "should be either `emitter` or `receiver`!"
            )

        if activity not in ["interactions", "actions"]:
            raise ValueError(
                "Wrong `activity` given, "
                "should be either `interactions` or `actions`!"
                f" The provided one is {activity}"
            )

        activity_count = self.get_analytics_count(
            day=day,
            activity=activity,
            author_id=author_id,
            activity_name=activity_name,
            activity_direction=activity_direction,
            filters=additional_filters,
        )

        return activity_count

    def get_analytics_count(
        self,
        day: date,
        activity: str,
        activity_name: str,
        author_id: str | int,
        activity_direction: str,
        **kwargs,
    ) -> list[RawAnalyticsItem]:
        """
        Gets the list of documents for the stated day

        Parameters
        ------------
        day : date
            a specific day date
        activity : str
            to be `interactions` or `actions`
        activity_name : str
            the activity name to do filtering
            could be `reply`, `reaction`, `mention, or ...
        author_id : str | int
            the author to do analytics on its data
        activity_direction : str
            the direction of activity
            could be `emitter` or `receiver`
        **kwargs : dict
            filters : dict[str, dict[str] | str]
                the filtering that we need to apply
                for default it is an None meaning
                no filtering would be applied

        Returns
        ---------
        activity_count : list[RawAnalyticsItem]
            raw analytics item which holds the user and
            the count of interaction in that day
        """
        filters: dict[str, dict[str, Any] | str] | None = kwargs.get("filters")
        start_day = datetime.combine(day, time(0, 0, 0))
        end_day = start_day + timedelta(days=1)

        match_filters = {
            "date": {"$gte": start_day, "$lt": end_day},
            "author_id": author_id,
        }
        if filters is not None:
            match_filters = {
                **match_filters,
                **filters,
            }

        pipeline = [
            {
                "$match": {
                    **match_filters,
                }
            },
            {"$unwind": f"${activity}"},
            {
                "$match": {
                    f"{activity}.name": activity_name,
                    f"{activity}.type": activity_direction,
                },
            },
            {"$unwind": f"${activity}.users_engaged_id"},
            {"$group": {"_id": f"${activity}.users_engaged_id", "count": {"$sum": 1}}},
        ]

        cursor = self.collection.aggregate(pipeline)
        db_result = list(cursor)
        activity_count = self._prepare_raw_analytics_item(author_id, db_result)

        return activity_count

    def _prepare_raw_analytics_item(
        self,
        author_id: str | int,
        activities_data: list[dict[str, str | int]],
    ) -> list[RawAnalyticsItem]:
        """
        post process the database results

        this will take the format `[{'_id': 9000, 'count': 4}]` and output a RawAnalyticsItem

        Parameters
        ------------
        author_id : str
            just for skipping self-interactions
        activities_data : dict[str, str | int]
            the user interaction count.
            the data will be as an example `[{'_id': 9000, 'count': 4}]`
            _id would be the users interacting with

        Returns
        --------
        raw_analytics : list[RawAnalyticsItem]
            the data in format of raw analytics item
        """
        analytics: list[RawAnalyticsItem] = []
        for data in activities_data:
            if data["_id"] != author_id:
                raw_analytics = RawAnalyticsItem(
                    account=data["_id"],  # type: ignore
                    count=data["count"],  # type: ignore
                )
                analytics.append(raw_analytics)
            else:
                # self interaction
                logging.info("Skipping self-interaction!")

        return analytics
