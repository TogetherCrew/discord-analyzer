#!/usr/bin/env python3
import logging
from datetime import timedelta, datetime
from typing import Any

import pymongo
from discord_analyzer.models.BaseModel import BaseModel


class RawInfoModel(BaseModel):
    def __init__(self, database=None):
        if database is None:
            logging.info("Database does not exist.")
            raise Exception("Database should not be None")
        super().__init__(collection_name="rawinfos", database=database)
        self.guild_msg = f"GUILDID: {self.database.name}:"
        self.validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "properties": {
                    "type": {"bsonType": "string"},
                    "author": {"bsonType": "string"},
                    "content": {"bsonType": "string"},
                    "user_Mentions": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"},
                    },
                    "roles_Mentions": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"},
                    },
                    "reactions": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "replied_User": {"bsonType": "string"},
                    "reference_Message": {"bsonType": "int"},
                    "datetime": {
                        "bsonType": "string",
                    },
                    "channelId": {
                        "bsonType": "string",
                    },
                },
            }
        }

    def get_first_date(self):
        """
        Get's the date of the first document in the collection
        For determining the analysis date ranges
        This is RawInfo specific method
        """
        if self.database[self.collection_name].count_documents({}) > 0:
            record = self.database[self.collection_name].find_one(
                {}, sort=[("createdDate", pymongo.ASCENDING)]
            )

            first_date = record["createdDate"]

            # (
            #     self.database[self.collection_name]
            #     .find()
            #     .sort([("createdDate", pymongo.ASCENDING)])
            #     .limit(1)[0]["createdDate"]
            # )
            # date_obj = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S")

            return first_date
            # do something with the first document
        else:
            # handle the case where no documents are returned by the query
            print(f"{self.guild_msg} No documents found in the collection")
            return None

    def get_day_entries(self, day: datetime, msg: str = "") -> list[dict[str, Any]]:
        """
        Gets the list of entries for the stated day
        This is RawInfo specific method

        `msg` parameter is for additional info to be logged
        """
        guild_msg = f"GUILDID: {self.database.name}:{msg}"

        start_day = day.replace(hour=0, minute=0, second=0)
        end_day = start_day + timedelta(days=1)

        logg_msg = f"{guild_msg} Fetching documents |"
        logg_msg += f" {self.collection_name}: {start_day} -> {end_day}"
        logging.info(logg_msg)

        entries = self.database[self.collection_name].find(
            {
                "$and": [
                    {"createdDate": {"$gte": start_day, "$lte": end_day}},
                    {"IsGeneratedByWebhook": False},
                ]
            }
        )
        return list(entries)
