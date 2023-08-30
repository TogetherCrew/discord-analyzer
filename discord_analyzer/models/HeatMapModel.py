#!/usr/bin/env python3
import logging
from datetime import datetime, timedelta, timezone

import pymongo

from discord_analyzer.models.BaseModel import BaseModel


class HeatMapModel(BaseModel):
    def __init__(self, database=None):
        if database is None:
            logging.exception("Database does not exist.")
            raise Exception("Database should not be None")
        super().__init__(collection_name="heatmaps", database=database)
        self.validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "properties": {
                    "date": {
                        "bsonType": "date",
                    },
                    "channel": {
                        "bsonType": "string",
                    },
                    "lone_messages": {
                        "bsonType": "array",
                        "items": {"bsonType": "int"},
                    },
                    "thr_messages": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "replier": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "replier_accounts": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["_id", "account", "count"],
                            "properties": {
                                "_id": {"bsonType": "string"},
                                "account": {"bsonType": "string"},
                                "count": {"bsonType": "int"},
                            },
                        },
                    },
                    "replied": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "mentioner": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "mentioner_accounts": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["_id", "account", "count"],
                            "properties": {
                                "_id": {"bsonType": "string"},
                                "account": {"bsonType": "string"},
                                "count": {"bsonType": "int"},
                            },
                        },
                    },
                    "mentioned": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "reacter": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "reacter_accounts": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["_id", "account", "count"],
                            "properties": {
                                "_id": {"bsonType": "string"},
                                "account": {"bsonType": "string"},
                                "count": {"bsonType": "int"},
                            },
                        },
                    },
                    "reacted": {"bsonType": "array", "items": {"bsonType": "int"}},
                    "account_name": {
                        "bsonType": "string",
                    },
                },
            }
        }

    def get_last_date(self):
        """
        Gets the date of the last document
        """
        try:
            date_str = (
                self.database[self.collection_name]
                .find()
                .sort([("date", pymongo.DESCENDING)])
                .limit(1)[0]["date"]
            )
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")

            return date_obj
            # Parsing the time and timezone
            date_str = date_str.split(" GMT")
            date_str[1] = "GMT" + date_str[1]
            date_str[1] = date_str[1].split(" ")[0].replace("GMT", "")
            zone = [date_str[1][0:3], date_str[1][3::]]
            zone_hrs = int(zone[0])
            zone_min = int(zone[1])
            date_obj = datetime.strptime(date_str[0], "%a %b %d %Y %H:%M:%S").replace(
                tzinfo=timezone(timedelta(hours=zone_hrs, minutes=zone_min))
            )
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
