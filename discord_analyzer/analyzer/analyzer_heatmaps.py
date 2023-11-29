import logging
from collections import Counter
from datetime import datetime, timedelta

# from analyzer.analyzer.base_analyzer import Base_analyzer
from discord_analyzer.analysis.activity_hourly import activity_hourly
from discord_analyzer.analyzer.heatmaps_utils import (
    get_bot_id,
    getNumberOfActions,
    store_counts_dict,
)
from discord_analyzer.DB_operations.mongo_neo4j_ops import MongoNeo4jDB
from discord_analyzer.models.GuildsRnDaoModel import GuildsRnDaoModel
from discord_analyzer.models.HeatMapModel import HeatMapModel
from discord_analyzer.models.RawInfoModel import RawInfoModel


class Heatmaps:
    def __init__(self, DB_connections: MongoNeo4jDB, testing: bool) -> None:
        self.DB_connections = DB_connections
        self.testing = testing

    def is_empty(self, guildId: str):
        """
        check whether the heatmaps for the guild is empty or not
        """
        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        heatmap_c = HeatMapModel(client[guildId])
        document = heatmap_c.get_one()

        return document is None

    def analysis_heatmap(self, guildId: str, from_start: bool = False):
        """
        Based on the rawdata creates and stores the heatmap data

        Parameters:
        -------------
        guildId : str
            the guild id to analyze data for
        from_start : bool
            do the analytics from scrach or not
            if True, if wouldn't pay attention to the existing data in heatmaps
            and will do the analysis from the first date


        Returns:
        ---------
        heatmaps_results : list of dictionary
            the list of data analyzed
            also the return could be None if no database for guild
              or no raw info data was available
        """
        # activity_hourly()
        guild_msg = f"GUILDID: {guildId}:"

        client = self.DB_connections.mongoOps.mongo_db_access.db_mongo_client

        if guildId not in client.list_database_names():
            logging.error(f"{guild_msg} Database {guildId} doesn't exist")
            logging.error(
                f"{guild_msg} Existing databases: {client.list_database_names()}"
            )  # flake8: noqa
            logging.info(f"{guild_msg} Continuing")
            return None

        # Collections involved in analysis
        # guild parameter is the name of the database
        rawinfo_c = RawInfoModel(client[guildId])
        heatmap_c = HeatMapModel(client[guildId])
        guild_rndao_c = GuildsRnDaoModel(client["Core"])

        # Testing if there are entries in the rawinfo collection
        if rawinfo_c.count() == 0:
            msg = f"{guild_msg} No entries in the collection"
            msg += "'rawinfos' in {guildId} databse"
            logging.warning(msg)
            return None

        if not heatmap_c.collection_exists():
            raise Exception(
                f"{guild_msg} Collection '{heatmap_c.collection_name}' does not exist"
            )
        if not rawinfo_c.collection_exists():
            raise Exception(
                f"{guild_msg} Collection '{rawinfo_c.collection_name}' does not exist"
            )

        last_date = heatmap_c.get_last_date()

        if last_date is None or from_start:
            # If no heatmap was created, than tha last date is the first
            # rawdata entry
            # last_date = rawinfo_c.get_first_date()
            last_date = guild_rndao_c.get_guild_period(guildId)
            if last_date is None:
                msg = f"{guild_msg} Collection"
                msg += f"'{rawinfo_c.collection_name}' does not exist"
                raise Exception(msg)
            # last_date.replace(tzinfo=timezone.utc)
        else:
            last_date = last_date + timedelta(days=1)

        # initialize the data array
        heatmaps_results = []

        # getting the id of bots
        bot_ids = get_bot_id(
            db_mongo_client=self.DB_connections.mongoOps.mongo_db_access.db_mongo_client,
            guildId=guildId,
        )

        while last_date.date() < datetime.now().date():
            entries = rawinfo_c.get_day_entries(last_date, "ANALYZER HEATMAPS: ")
            if len(entries) == 0:
                # analyze next day
                last_date = last_date + timedelta(days=1)
                continue

            prepared_list = []
            account_list = []

            for entry in entries:
                if "replied_user" not in entry:
                    reply = ""
                else:
                    reply = entry["replied_user"]

                # eliminating bots
                if entry["author"] not in bot_ids:
                    prepared_list.append(
                        {
                            # .strftime('%Y-%m-%d %H:%M'),
                            "datetime": entry["createdDate"],
                            "channel": entry["channelId"],
                            "author": entry["author"],
                            "replied_user": reply,
                            "user_mentions": entry["user_mentions"],
                            "reactions": entry["reactions"],
                            "threadId": entry["threadId"],
                            "mess_type": entry["type"],
                        }
                    )
                    if entry["author"] not in account_list:
                        account_list.append(entry["author"])

                    if entry["user_mentions"] is not None:
                        for account in entry["user_mentions"]:
                            # for making the line shorter
                            condition2 = account not in bot_ids
                            if account not in account_list and condition2:
                                account_list.append(account)

            activity = activity_hourly(prepared_list, acc_names=account_list)
            # # activity[0]
            # heatmap = activity[1][0]
            # Parsing the activity_hourly into the dictionary
            results = self._post_process_data(activity[1], len(account_list))
            heatmaps_results.extend(results)

            # analyze next day
            last_date = last_date + timedelta(days=1)

        return heatmaps_results

    def _post_process_data(self, heatmap_data, accounts_len):
        results = []
        for heatmap in heatmap_data:
            for i in range(accounts_len):
                heatmap_dict = {}
                heatmap_dict["date"] = heatmap["date"][0]
                heatmap_dict["channelId"] = heatmap["channel"][0]
                heatmap_dict["thr_messages"] = heatmap["thr_messages"][i]
                heatmap_dict["lone_messages"] = heatmap["lone_messages"][i]
                heatmap_dict["replier"] = heatmap["replier"][i]
                heatmap_dict["replied"] = heatmap["replied"][i]
                heatmap_dict["mentioner"] = heatmap["mentioner"][i]
                heatmap_dict["mentioned"] = heatmap["mentioned"][i]
                heatmap_dict["reacter"] = heatmap["reacter"][i]
                heatmap_dict["reacted"] = heatmap["reacted"][i]
                heatmap_dict["reacted_per_acc"] = store_counts_dict(
                    dict(Counter(heatmap["reacted_per_acc"][i]))
                )
                heatmap_dict["mentioner_per_acc"] = store_counts_dict(
                    dict(Counter(heatmap["mentioner_per_acc"][i]))
                )
                heatmap_dict["replied_per_acc"] = store_counts_dict(
                    dict(Counter(heatmap["replied_per_acc"][i]))
                )
                heatmap_dict["account_name"] = heatmap["acc_names"][i]
                sum_ac = getNumberOfActions(heatmap_dict)

                if not self.testing and sum_ac > 0:
                    results.append(heatmap_dict)

        return results
