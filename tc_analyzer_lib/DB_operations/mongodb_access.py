from tc_analyzer_lib.utils.mongo import MongoSingleton


class DB_access:
    def __init__(self, db_name) -> None:
        """
        set-up the MongoDB database access

        Parameters:
        ------------
        db_name : str
           the exact guildId to use
           if `None`, the DB_access.db_client will be `None` but
            DB_access.db_mongo_client will be available to use
           else both `DB_access.db_client` and `
            DB_access.db_mongo_client` are avaialble to use

           the `db_client` has a specific access to the guild (db_name)
           the `db_mongo_client` has more variety of access which
            can be used to access to the whole databases (guilds)
        connection_string : str
           the connection string used to connect to MongoDB
        """

        client = MongoSingleton.get_instance().get_client()
        self.db_name = db_name
        #   if db_name is None:
        #       self.db_client = None
        #   else:
        #       self.db_client = client[db_name]

        self.db_mongo_client = client

    def _db_call(self, calling_function, query, feature_projection=None, sorting=None):
        """
        call the function on database, it could be whether aggragation or find
        Parameters:
        -------------
        calling_function : function
           can be `MongoClient.find` or `MongoClient.aggregate`
        query : dictionary
           the query as a dictionary
        feature_projection : dictionary
           the dictionary to or not to project the results on it
           default is None, meaning to return all features
        sorting : tuple
           sort the results base on the input dictionary
           if None, then do not sort the results

        Returns:
        ----------
        cursor : mongodb Cursor
           cursor to get the information of a query
        """
        # if there was no projection available
        if feature_projection is None:
            # if sorting was given
            if sorting is not None:
                cursor = calling_function(query).sort(sorting[0], sorting[1])
            else:
                cursor = calling_function(query)
        else:
            if sorting is not None:
                cursor = calling_function(query, feature_projection).sort(
                    sorting[0], sorting[1]
                )
            else:
                cursor = calling_function(query, feature_projection)

        return cursor

    def query_db_aggregation(self, table, query, feature_projection=None, sorting=None):
        """
        do aggregation operation the database using query

        Parameters:
        ------------
        table : string
           the table name to retrieve the data
        query : dictionary
           the query as a dictionary
        feature_projection : dictionary
           the dictionary to or not to project the results on it
           default is None, meaning to return all features
        sorting : tuple
           sort the results base on the input dictionary
           if None, then do not sort the results

        Returns:
        ----------
        cursor : mongodb Cursor
           cursor to get the information of a query
        """

        cursor = self._db_call(
            calling_function=self.db_mongo_client[self.db_name][table].aggregate,
            query=query,
            feature_projection=feature_projection,
            sorting=sorting,
        )

        return cursor

    def query_db_find(self, table, query, feature_projection=None, sorting=None):
        """
        do find operation the database using query

        Parameters:
        ------------
        table : string
           the table name to retrieve the data
        query : dictionary
           the query as a dictionary
        feature_projection : dictionary
           the dictionary to or not to project the results on it
           default is None, meaning to return all features
        sorting : tuple
           sort the results base on the input dictionary
           if None, then do not sort the results

        Returns:
        ----------
        cursor : mongodb Cursor
           cursor to get the information of a query
        """
        cursor = self._db_call(
            calling_function=self.db_mongo_client[self.db_name][table].find,
            query=query,
            feature_projection=feature_projection,
            sorting=sorting,
        )
        return cursor
