#!/usr/bin/env python3


import logging

from discord_analyzer.models.BaseModel import BaseModel


class RnDaoModel(BaseModel):
    def __init__(self, database=None):
        if database is None:
            logging.exception("Database does not exist.")
            raise Exception("Database should not be None")
        super().__init__(collection_name="RnDAO", database=database)
