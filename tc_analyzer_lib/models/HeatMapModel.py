#!/usr/bin/env python3
from datetime import datetime

from pymongo import DESCENDING
from pymongo.database import Database
from tc_analyzer_lib.models.BaseModel import BaseModel


class HeatMapModel(BaseModel):
    def __init__(self, database: Database):
        super().__init__(collection_name="heatmaps", database=database)

    def get_last_date(self):
        """
        Gets the date of the last document
        """
        try:
            date_str = (
                self.database[self.collection_name]
                .find()
                .sort([("date", DESCENDING)])
                .limit(1)[0]["date"]
            )
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")

            return date_obj
        except Exception as e:
            print(e)
            return None

    def get_channels_disctinct(self):
        """
        get the unique channels available in heatmaps

        Returns:
        ----------
        distinct_channels : array of str
            the returned data distincted
        """
        feature_projection = {"channelId": 1}

        try:
            cursor = (
                self.database[self.collection_name]
                .find(projection=feature_projection)
                .distinct("channelId")
            )
            data = list(cursor)
        except Exception as e:
            print("Couldn't retreve distinct channels, exception: ", e)
            data = None

        return data

    def remove_all_data(self):
        """
        Removes all data whithing the collection

        Note: this is a dangerous function,
            since it deletes all data whithin memberactivity collection.

        Returns:
        -----------
        state : bool
            if True, the data whithin collection is successfully deleted
            if False, an exception is happened
        """
        try:
            self.database[self.collection_name].delete_many({})
            return True
        except Exception as e:
            print(e)
            return False
