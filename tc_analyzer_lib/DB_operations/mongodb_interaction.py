import logging

from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access


class MongoDBOps:
    def __init__(self):
        """
        mongoDB database operations
        """
        self.DB_access = DB_access
        self.guild_msg = ""

    def set_mongo_db_access(self, platform_id=None):
        """
        set a database access to a specific guild

        if platform_id was `None` then the mongo_db_access just
         have the `db_mongo_client` to use
        but if wasn't then mongo_db_access
         would also have db_client which is connected to a guild
        """
        self.mongo_db_access = self.DB_access(db_name=platform_id)
        self.guild_msg = f"PLATFORMID: {platform_id}:"

    def _do_analytics_write_transaction(
        self,
        platform_id,
        delete_heatmaps,
        delete_member_acitivities,
        acitivties_list,
        heatmaps_list,
        batch_size=1000,
    ):
        """
        do write operations in a transaction.
        this transaction contains deleting data and insertion in a transaction

        Parameters:
        ------------
        delete_heatmaps : bool
            delete the heatmap data or not
        delete_member_acitivities : bool
            delete the memberactivities data or not
        acitivties_list : list of dict
            list of memberactivity data to store
        heatmaps_list : list of dict
            list of heatmap data to store
        """

        def callback_wrapper(session):
            self._session_custom_transaction(
                session,
                platform_id,
                delete_heatmaps,
                delete_member_acitivities,
                acitivties_list,
                heatmaps_list,
                batch_size,
            )

        with self.mongo_db_access.db_mongo_client.start_session() as session:
            session.with_transaction(
                callback=callback_wrapper,
                read_concern=ReadConcern("local"),
                write_concern=WriteConcern("local"),
            )

    def _session_custom_transaction(
        self,
        session,
        platform_id,
        delete_heatmaps,
        delete_member_acitivities,
        memberactiivties_list,
        heatmaps_list,
        batch_size=1000,
    ):
        """
        our custom transaction function
        which contains the deletion of heatmaps and memberactivities
          also insertion of activities_list and heatmaps_list after

        """
        self.guild_msg = f"PLATFORMID: {platform_id}:"

        if delete_heatmaps:
            logging.info(f"{self.guild_msg} Removing Heatmaps data!")
            self.empty_collection(
                session=session, platform_id=platform_id, activity="heatmaps"
            )
        if delete_member_acitivities:
            logging.info(f"{self.guild_msg} Removing MemberActivities MongoDB data!")
            self.empty_collection(
                session=session, platform_id=platform_id, activity="memberactivities"
            )

        if memberactiivties_list is not None and memberactiivties_list != []:
            self.insert_into_memberactivities_batches(
                session=session,
                acitivities_list=memberactiivties_list,
                platform_id=platform_id,
                batch_size=batch_size,
            )

        if heatmaps_list is not None and heatmaps_list != []:
            self.insert_into_heatmaps_batches(
                session=session,
                heatmaps_list=heatmaps_list,
                platform_id=platform_id,
                batch_size=batch_size,
            )

    def insert_into_memberactivities_batches(
        self, session, acitivities_list, platform_id, batch_size=1000
    ):
        """
        insert data into memberactivities collection of mongoDB in batches

        Parameters:
        ------------
        acitivities_list : list of dictionaries
            a list of activities to be imported to memberactivities table
        batch_size : int
            the count of data in batches
            default is 1000
        platform_id : str
            the platform_id to insert data to it
        """
        memberactivities_collection = session.client[platform_id].memberactivities
        self._batch_insertion(
            collection=memberactivities_collection,
            data=acitivities_list,
            message=f"{self.guild_msg} Inserting memberactivities documents to MongoDB",
            batch_size=batch_size,
        )

    def insert_into_heatmaps_batches(
        self, session, heatmaps_list, platform_id, batch_size=1000
    ):
        """
        insert data into heatmaps collection of mongoDB in batches

        Parameters:
        ------------
        heatmaps_list : list of dictionaries
            a list of activities to be imported to memberactivities table
        batch_size : int
            the count of data in batches
            default is 1000
        platform_id : str
            the platform_id to insert data to it
        """
        heatmaps_collection = session.client[platform_id].heatmaps

        self._batch_insertion(
            heatmaps_collection,
            heatmaps_list,
            message=f"{self.guild_msg} Inserting heatmaps documents to mongoDB",
            batch_size=batch_size,
        )

    def _batch_insertion(self, collection, data, message, batch_size):
        """
        do the batch insertion with and log a given message

        Parameters:
        -------------
        collection : MongoDB collection
            the collection to insert data into
        data : list
            data to insert into the collection
        message : str
            the additional message to log while insertion
        batch_size : int
            the count of data in batches
        """
        data_len = len(data)
        batch_count = data_len // batch_size

        for loop_idx, batch_idx in enumerate(range(0, data_len, batch_size)):
            logging.info(f"{message}: Batch {loop_idx + 1}/{batch_count}")
            collection.insert_many(data[batch_idx : batch_idx + batch_size])

    def empty_collection(self, session, platform_id, activity):
        """
        empty a specified collection

        Parameters:
        -------------
        session : mongoDB session
            the session to needed to delete the data
        platform_id : str
            the platform_id to remove its collection data
        activity : str
            `memberactivities` or `heatmaps` or other collections
            the collection to access and delete its data

        Returns:
        ---------
        `None`
        """
        if activity == "heatmaps":
            collection = session.client[platform_id].heatmaps
        elif activity == "memberactivities":
            collection = session.client[platform_id].memberactivities
        else:
            raise NotImplementedError(
                "removing heatmaps or memberactivities are just implemented!"
            )

        collection.delete_many({})
