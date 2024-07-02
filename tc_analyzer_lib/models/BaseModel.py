#!/usr/bin/env python3
import logging

from pymongo.database import Database


class BaseModel:
    """
    BaseModel description
    All integrated models inherit from this object
    """

    def __init__(self, collection_name: str, database: Database):
        self.collection_name = collection_name
        self.database = database
        self.collection = database[collection_name]
        self.exists = False

    def collection_exists(self):
        """
        Collection presence test
        returns True if collection with this name exists in the
        database
        """
        if self.collection_name in self.database.list_collection_names():
            return True
        else:
            return False

    def insert_one(self, obj_dict):
        """
        Inserts one document into the defined collection
        """

        if not self.collection_exists():
            msg = "Inserting guild object into the"
            msg += f" {self.collection_name} collection failed:"
            msg += "Collection does not exist"
            logging.info(msg)
            return
        logging.info(
            f"Inserting guild object into the {self.collection_name} collection."
        )

        return self.collection.insert_one(obj_dict)

    def insert_many(self, obj_dict_arr):
        """
        Inserts one document into the defined collection
        If create is True then a new collection is created
        """

        if not self.collection_exists():
            msg = "Inserting many guild objects into the"
            msg += f"{self.collection_name} collection failed: "
            msg += "Collection does not exist"
            logging.info(msg)
            return
        self.collection = self.database[self.collection_name]
        msg = "Inserting many guild objects into the "
        msg += f"{self.collection_name} collection."
        logging.info(msg)

        return self.collection.insert_many(obj_dict_arr)

    def _create_collection_if_not_exists(self):
        """
        Creates the collection with specified name if it does not exist
        """
        logging.info(f"Check if collection {self.collection_name} exists in database")
        if self.collection_name in self.database.list_collection_names():
            logging.info(f"Collection {self.collection_name} exists")
        else:
            logging.info(f"Collection {self.collection_name} doesn't exist")
            result = self.database.create_collection(self.collection_name)
            logging.info(result)
        self.database.command("collMod", self.collection_name)
        self.collection = self.database[self.collection_name]
        self.exists = True

    def get_one(self):
        """
        Gets one documents from the database,
        For testing purposes, no filtering is implemented.
        """
        return self.database[self.collection_name].find_one()

    def get_all(self):
        """
        Gets all documents from the database
        """

        return self.database[self.collection_name].find()

    def count(self):
        """
        Returns the number of entries in this collection
        """
        return self.database[self.collection_name].count_documents({})
