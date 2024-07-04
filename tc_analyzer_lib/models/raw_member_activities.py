#!/usr/bin/env python3
import logging
from datetime import datetime, time, timedelta

import numpy as np
from tc_analyzer_lib.models.BaseModel import BaseModel
from tc_analyzer_lib.utils.mongo import MongoSingleton


class RawMemberActivities(BaseModel):
    def __init__(self, platform_id: str):
        client = MongoSingleton.get_instance().get_client()
        super().__init__(
            collection_name="rawmemberactivities", database=client[platform_id]
        )
        self.msg_prefix = f"PLATFORMID: {platform_id}:"

    def get_hourly_analytics(
        self,
        day: datetime.date,
        activity: str,
        filters: dict[str, dict[str] | str] | None = None,
        msg: str = "",
    ) -> list[int]:
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
        hourly_analytics : list[int]
            a vector with length of 24
            each index representing the count of activity for that day
        """
        prefix = f"{self.msg_prefix} {msg}"

        if activity not in ["interactions", "actions"]:
            raise ValueError(
                f"{prefix} Wrong activity given!"
                " Should be either `interactions`, or `actions`"
            )

        start_day = datetime.combine(day, time(0, 0, 0))
        end_day = start_day + timedelta(days=1)

        logg_msg = f"{prefix} Fetching documents |"
        logg_msg += f" {self.collection_name}: {start_day} -> {end_day}"
        logging.info(logg_msg)

        pipeline = [
            # the day for analytics
            {"$match": {"date": {"$gte": start_day, "$lt": end_day}}},
            # Unwind the activity array
            {"$unwind": f"${activity}"},
        ]
        if filters is not None:
            pipeline.append(
                {"$match": filters},
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
            if hour < 0 or hour > 24:
                raise ValueError("Wrong hour given from mongodb query!")
            activity_count = analytics["count"]

            hourly_analytics[hour] = activity_count

        return list(hourly_analytics)
