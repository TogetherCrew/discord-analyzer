#!/usr/bin/env python3
import logging

import pymongo
from tc_analyzer_lib.models.BaseModel import BaseModel


class MemberActivityModel(BaseModel):
    def __init__(self, database=None):
        if database is None:
            logging.exception("Database does not exist.")
            raise Exception("Database should not be None")
        super().__init__(collection_name="memberactivities", database=database)

    def get_last_date(self):
        """
        Gets the date of the last document
        """
        try:
            date = (
                self.database[self.collection_name]
                .find()
                .sort([("date", pymongo.DESCENDING)])
                .limit(1)[0]["date"]
            )
            return date
        except Exception as e:
            print(e)
            return None

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
