import logging
from datetime import datetime, timedelta

from discord_analyzer.analysis.compute_member_activity import compute_member_activity
from discord_analyzer.analyzer.memberactivity_utils import MemberActivityUtils
from discord_analyzer.DB_operations.mongo_neo4j_ops import MongoNeo4jDB
from discord_analyzer.models.MemberActivityModel import MemberActivityModel
from discord_analyzer.models.RawInfoModel import RawInfoModel


class MemberActivities:
    def __init__(self, DB_connections: MongoNeo4jDB) -> None:
        self.DB_connections = DB_connections

        self.utils = MemberActivityUtils(DB_connections)

    def analysis_member_activity(self, guildId, mongo_connection_str, from_start=False):
        """
        Based on the rawdata creates and stores the member activity data

        Parameters:
        -------------
        guildId : str
            the guild id to analyze data for
        from_start : bool
            do the analytics from scrach or not
            if True, if wouldn't pay attention to the existing data in memberactivities
            and will do the analysis from the first date

        Returns:
        ---------
        memberactivity_results : list of dictionary
            the list of data analyzed
            also the return could be None if no database for guild
              or no raw info data was available
        memberactivity_networkx_results : list of networkx objects
            the list of data analyzed in networkx format
            also the return could be None if no database for guild
             or no raw info data was available
        """
        guild_msg = f"GUILDID: {guildId}:"

        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        # check current guild is exist
        if guildId not in client.list_database_names():
            logging.error(f"{guild_msg} Database {guildId} doesn't exist")
            logging.error(f"{guild_msg} No such databse!")
            logging.info(f"{guild_msg} Continuing")
            return (None, None)

        member_activity_c = MemberActivityModel(client[guildId])
        rawinfo_c = RawInfoModel(client[guildId])

        # Testing if there are entries in the rawinfo collection
        if rawinfo_c.count() == 0:
            logging.warning(
                f"No entries in the collection 'rawinfos' in {guildId} databse"
            )
            return (None, None)

        # get current guild_info
        guild_info = self.utils.get_one_guild(guildId)

        channels, window, action = (
            guild_info["metadata"]["selectedChannels"],
            guild_info["metadata"]["window"],
            guild_info["metadata"]["action"],
        )
        period = guild_info["metadata"]["period"]

        channels = list(map(lambda x: x["channelId"], channels))

        # get date range to be analyzed
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        logging.info(f"{guild_msg} memberactivities Analysis started!")

        # initialize
        load_past_data = False

        # if we had data from past to use
        if member_activity_c.count() != 0:
            load_past_data = True

        load_past_data = load_past_data and not from_start

        # first_date = rawinfo_c.get_first_date().replace(
        #     hour=0, minute=0, second=0, microsecond=0
        # )

        first_date = period
        if first_date is None:
            logging.error(f"No guild: {guildId} available in Platforms.core!")
            return None, None

        last_date = today - timedelta(days=1)

        date_range = [first_date, last_date]

        if load_past_data:
            # num_days_to_load = (
            #       max([CON_T_THR, VITAL_T_THR, STILL_T_THR, PAUSED_T_THR])+1
            # ) * WINDOW_D
            num_days_to_load = (
                max(
                    [
                        action["CON_T_THR"],
                        action["VITAL_T_THR"],
                        action["STILL_T_THR"],
                        action["PAUSED_T_THR"],
                    ]
                )
                + 1
            ) * window[0]
            date_range[0] = date_range[1] - timedelta(days=num_days_to_load)

            # if the date range goes back more than the "7 days `period` forward"
            if date_range[0] < period + timedelta(days=window[0]):
                date_range[0] = period + timedelta(days=window[0])

        # get all users during date_range
        all_users = self.utils.get_all_users(guildId)
        # change format like 23/03/27
        date_range = [dt.strftime("%y/%m/%d") for dt in date_range]

        networkx_objects, activities = compute_member_activity(
            guildId,
            mongo_connection_str,
            channels,
            all_users,
            date_range,
            window,
            action,
            load_past_data=load_past_data,
        )

        if not from_start:
            # first date of storing the data
            first_storing_date = member_activity_c.get_last_date()
            activities = self.utils.refine_memberactivities_data(
                activities, first_storing_date
            )

        memberactivity_results = activities
        memberactivity_networkx_results = networkx_objects

        return memberactivity_results, memberactivity_networkx_results
