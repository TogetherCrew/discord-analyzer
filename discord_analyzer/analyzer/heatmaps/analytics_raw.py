from datetime import datetime, time, timedelta
import numpy as np

from utils.mongo import MongoSingleton
from discord_analyzer.schemas import RawAnalyticsItem


class AnalyticsRaw:
    def __init__(self, platform_id: str) -> None:
        client = MongoSingleton.get_instance().get_client()
        # `rawmemberactivities` is the collection we would use for analytics
        self.collection = client[platform_id]["rawmemberactivities"]
        self.msg_prefix = f"PLATFORMID: {platform_id}:"

    def analyze(
        self,
        day: datetime.date,
        activity: str,
        activity_name: str,
        activity_direction: str,
        author_id: int,
        **kwargs,
    ) -> RawAnalyticsItem | None:
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
                "should be either `interactions` or `actions`"
            )

        activity_count = self.get_analytics_count(
            day=day,
            activity=activity,
            author_id=author_id,
            filters={
                f"{activity}.name": activity_name,
                f"{activity}.type": activity_direction,
                **additional_filters,
            },
        )

        return activity_count

    def get_analytics_count(
        self,
        day: datetime.date,
        activity: str,
        author_id: str,
        filters: dict[str, dict[str] | str] | None = None,
    ) -> RawAnalyticsItem | None:
        """
        Gets the list of documents for the stated day

        Parameters
        ------------
        day : datetime.date
            a specific day date
        activity : str
            to be `interactions` or `actions`
        filter : dict[str, dict[str] | str] | None
            the filtering that we need to apply
            for default it is an None meaning
            no filtering would be applied
        msg : str
            additional information to be logged
            for default is empty string meaning no additional string to log

        Returns
        ---------
        activity_count : RawAnalyticsItem
            raw analytics item which holds the user and
            the count of interaction in that day
        """
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
            # the day for analytics
            {
                "$match": {
                    **match_filters,
                }
            },
            # Unwind the activity array
            {"$unwind": f"${activity}"},
            # Add a field for the hour of the day from the date field
            {"$addFields": {"date": "$date"}},
            # Group by the hour and count the number of activity
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
                    "count": {"$sum": 1},
                }
            },
            # Project the results into the desired format
            {"$sort": {"_id": 1}},  # sorted by hour
        ]

        cursor = self.collection.aggregate(pipeline)
        db_result = list(cursor)
        if db_result != []:
            activity_count = self._prepare_raw_analytics_item(db_result[0], author_id)
        else:
            activity_count = None

        return activity_count

    def _prepare_raw_analytics_item(
        self,
        activity_data: dict[str, str | int],
        author_id: str,
    ) -> RawAnalyticsItem:
        """
        post process the database results

        this will take the format `[{'_id': '2023-01-01', 'count': 4}]` and output a RawAnalyticsItem

        Parameters
        ------------
        activity_data : dict[str, str | int]
            the user interaction count.
            the data will be as an example `[{'_id': '2023-01-01', 'count': 4}]`
        author_id : str
            the author that had the count of activity

        Returns
        --------
        raw_analytics_item : RawAnalyticsItem
            the data in format of raw analytics item we've already made
        """
        raw_analytics_item = RawAnalyticsItem(
            account=author_id,
            count=activity_data["count"],
        )

        return raw_analytics_item
