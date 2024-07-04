from datetime import date, datetime, time, timedelta
from typing import Any

import numpy as np
from tc_analyzer_lib.utils.mongo import MongoSingleton


class AnalyticsHourly:
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
        author_id: str | int,
        **kwargs,
    ) -> list[int]:
        """
        analyze the hourly the messages

        Parameters
        ------------
        day : date
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
        """
        additional_filters: dict[str, str] = kwargs.get("additional_filters", {})

        if activity_direction not in ["emitter", "receiver"]:
            raise AttributeError(
                "Wrong activity_direction given, "
                "should be either `emitter` or `receiver`!"
            )

        if activity not in ["interactions", "actions"]:
            raise AttributeError(
                "Wrong `activity` given, "
                "should be either `interactions` or `actions`"
            )

        activity_vector = self.get_hourly_analytics(
            day=day,
            activity=activity,
            author_id=author_id,
            filters={
                f"{activity}.name": activity_name,
                f"{activity}.type": activity_direction,
                **additional_filters,
            },
        )

        return activity_vector

    def get_hourly_analytics(
        self,
        day: date,
        activity: str,
        author_id: str | int,
        filters: dict[str, dict[str, Any] | str] | None = None,
    ) -> list[int]:
        """
        Gets the list of documents for the stated day

        Parameters
        ------------
        day : date
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
        hourly_analytics : list[int]
            a vector with length of 24
            each index representing the count of activity for that day
        """
        start_day = datetime.combine(day, time(0, 0, 0))
        end_day = start_day + timedelta(days=1)

        pipeline = [
            # the day for analytics
            {
                "$match": {
                    "date": {"$gte": start_day, "$lt": end_day},
                    "author_id": author_id,
                }
            },
            # Unwind the activity array
            {"$unwind": f"${activity}"},
        ]
        if filters is not None:
            pipeline.append(
                {"$match": filters},
            )

        # we need to count each enaged user as an interaction
        if activity == "interactions":
            pipeline.extend(
                [
                    {"$unwind": "$interactions.users_engaged_id"},
                    # ignoring self-interactions
                    {
                        "$match": {
                            "$expr": {
                                "$ne": ["$interactions.users_engaged_id", "$author_id"]
                            }
                        }
                    },
                ]
            )

        pipeline.extend(
            [
                # Add a field for the hour of the day from the date field
                {"$addFields": {"hour": {"$hour": "$date"}}},
                # Group by the hour and count the number of mentions
                {"$group": {"_id": "$hour", "count": {"$sum": 1}}},
                # Project the results into the desired format
                {"$sort": {"_id": 1}},  # sorted by hour
            ]
        )

        # Execute the aggregation pipeline
        cursor = self.collection.aggregate(pipeline)
        results = list(cursor)

        hourly_analytics = self._process_vectors(results)
        return hourly_analytics

    def _process_vectors(
        self, analytics_mongo_results: list[dict[str, int]]
    ) -> list[int]:
        """
        post process the mongodb query aggregation results

        Parameters
        ------------
        analytics_mongo_results : list[dict[str, int]]
            the mongodb query aggregation results
            the format of the data should be as below
            `[{'_id': 0, 'count': 2}, {'_id': 1, 'count': 1}, ...]`
            the `_id` is hour and `count` is the count of user activity

        Returns
        ---------
        hourly_analytics : list[int]
            a vector with length of 24
            each index representing the count of actions/interactions for that day
        """
        hourly_analytics = np.zeros(24)

        for analytics in analytics_mongo_results:
            hour = analytics["_id"]
            activity_count = analytics["count"]

            hourly_analytics[hour] = activity_count

        return list(hourly_analytics)
