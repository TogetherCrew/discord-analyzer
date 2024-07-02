#!/usr/bin/env python3
import logging
from datetime import datetime, timedelta
from typing import Any

from pymongo import ASCENDING
from pymongo.database import Database
from tc_analyzer_lib.models.BaseModel import BaseModel


class RawInfoModel(BaseModel):
    def __init__(self, database: Database):
        super().__init__(collection_name="rawmemberactivities", database=database)
        self.guild_msg = f"PLATFORMID: {self.database.name}:"

    def get_first_date(self):
        """
        Get's the date of the first document in the collection
        For determining the analysis date ranges
        This is RawInfo specific method
        """
        if self.database[self.collection_name].count_documents({}) > 0:
            record = self.database[self.collection_name].find_one(
                {}, sort=[("createdDate", ASCENDING)]
            )

            first_date = record["createdDate"]
            return first_date
        else:
            # handle the case where no documents are returned by the query
            logging.info(f"{self.guild_msg} No documents found in the collection")
            return None

    def get_day_entries(self, day: datetime, msg: str = "") -> list[dict[str, Any]]:
        """
        Gets the list of entries for the stated day
        This is RawInfo specific method

        `msg` parameter is for additional info to be logged
        """
        guild_msg = f"PLATFORMID: {self.database.name}:{msg}"

        start_day = day.replace(hour=0, minute=0, second=0)
        end_day = start_day + timedelta(days=1)

        logg_msg = f"{guild_msg} Fetching documents |"
        logg_msg += f" {self.collection_name}: {start_day} -> {end_day}"
        logging.info(logg_msg)

        entries = self.database[self.collection_name].find(
            {
                "$and": [
                    {"createdDate": {"$gte": start_day, "$lte": end_day}},
                    {"isGeneratedByWebhook": False},
                ]
            }
        )
        return list(entries)
