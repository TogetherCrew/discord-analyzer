class MongodbQuery:
    def __init__(self) -> None:
        """
        create different queries to query the database
        """
        pass

    def _check_inputs(
        self,
        acc_names,
        channels,
        dates,
        variable_aggregation_type="and",
        value_aggregation_type="or",
    ):
        """
        just check whether the inputs are correctly entered or not
        """
        # checking the length of arrays
        if len(acc_names) < 1:
            raise ValueError("acc_names array is empty!")
        if len(channels) < 1:
            raise ValueError("channels array is empty!")
        if len(dates) < 1:
            raise ValueError("dates array is empty!")

        # checking the variable aggregation_type variable
        if variable_aggregation_type not in ["and", "or"]:
            raise ValueError(
                f"variable aggregation type must be either `and` or \
                  `or`!\nentered value is:{variable_aggregation_type}"
            )

        # checking the value aggregation_type variable
        if value_aggregation_type not in ["and", "or"]:
            raise ValueError(
                f"value aggregation type must be either `and` or \
                      `or`!\nentered value is:{value_aggregation_type}"
            )

    def create_query_filter_account_channel_dates(
        self,
        acc_names,
        channels,
        dates,
        variable_aggregation_type="and",
        value_aggregation_type="or",
        date_key="date",
        channel_key="channelId",
        account_key="account_name",
    ):
        """
        A query to filter the database on account_name,
         and/or channel_names, and/or dates.
        the aggregation of varibales (`account_name`, `channels`, and `dates`)
         can be set to `and` or `or`

        Parameters:
        ------------
        acc_names : list of string
            each string is an account name that needs to be included.
            The minimum length of this list is 1
        channels : list of string
            each string is a channel identifier for
              the channels that need to be included.
            The minimum length of this list is 1
        dates : list of datetime
            each datetime object is a date that needs to be included.
            The minimum length of this list is 1
            should be in type of `%Y-%m-%d` which is the exact database format
        variable_aggregation_type : string
            values can be [`and`, `or`], the aggregation type between the variables
                  (variables are `acc_names`, `channels`, and `dates`)
            `or` represents the or between the queries of acc_name, channels, dates
            `and` represents the and between the queries of acc_name, channels, dates
            default value is `and`
        value_aggregation_type : string
            values can be [`and`, `or`], the aggregation type between the
             values of each variable
            `or` represents the `or` operation between the values of input arrays
            `and` represents the `and` operation between the values of input arrays
            default value is `or`
        date_key : string
            the name of the field of date in database
            default is `date`
        channel_key : string
            the id of the field of channel name in database
            default is `channelId`
        account_key : string
            the name of the field account name in the database
            default is `account_name`
        Returns:
        ----------
        query : dictionary
            the query to get access
        """

        # creating each part of query seperately

        # creating date query
        date_query = []
        for date in dates:
            date_query.append({date_key: {"$regex": date}})

        # creating channels query
        channel_query = []

        for ch in channels:
            channel_query.append({channel_key: ch})

        # creating the account_name query
        account_query = []

        for account in acc_names:
            account_query.append({account_key: account})

        # creating the query
        query = {
            "$"
            + variable_aggregation_type: [
                {"$" + value_aggregation_type: account_query},
                {"$" + value_aggregation_type: channel_query},
                # for time we should definitly use `or` because
                # `and` would result in nothing!
                {"$or": date_query},
            ]
        }

        return query

    def create_query_channel(self, channels_name):
        """
        create a dictionary of query to get channel_id using channel_name
        Parameters:
        -------------
        channel_name : list
            a list of channel names to retrive their id

        Returns:
        ---------
        query : dictionary
            the query to retrieve the channel ids
        """
        query_channelId = {"channel": {"$in": channels_name}}

        return query_channelId

    def create_query_threads(
        self, channels_id, dates, channelsId_key="channelId", date_key="date"
    ) -> dict:
        """
        create a dictionary of query to query the DB,
         getting the messages for specific channels and dates
        Parameters:
        ------------
        channels_id : list
            list of strings, each string is a channel
             identifier for the channels that needs to be included.
            The minimum length of this list is 1
        dates : list
            list of datetime objects, each datetime
             object is a date that needs to be included.
            The minimum length of this list is 1
        channelsId_key : string
            the field name corresponding to chnnel id in database
            default value is `channelId`
        date_key : string
            the field name corresponding to date in database
            default value is `date`

        Returns:
        ---------
        query : dictionary
            a dictionary that query the database
        """
        # Array inputs checking
        if len(channels_id) < 1:
            raise ValueError("channels_id array is empty!")
        if len(dates) < 1:
            raise ValueError("dates array is empty!")

        datetime_query = []
        for date in dates:
            datetime_query.append({date_key: {"$regex": date}})

        query = {
            "$and": [
                {channelsId_key: {"$in": channels_id}},
                {"$or": datetime_query},
                # do not return the messages with no thread
                {"thread": {"$ne": "None"}},
            ]
        }

        return query
