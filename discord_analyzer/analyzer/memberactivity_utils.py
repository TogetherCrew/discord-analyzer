import logging

from dateutil import parser


class MemberActivityUtils:
    def __init__(self, DB_connection) -> None:
        self.DB_connection = DB_connection

    def refine_memberactivities_data(self, all_member_activities, first_date):
        """
        refine the data of memberactivities (don't use the data that are not needed)
        it would save the data from the first_date

        Parameters:
        --------------
        all_member_activities : array of dict
            the memberactivities for the whole date
        first_date : datetime
            the first date of saving date
            we would use this to specify the exact data activity to save
        """

        data_to_save = []
        for activity in all_member_activities:
            if first_date is None or parser.parse(activity["date"]) > first_date:
                data_to_save.append(activity)

        return data_to_save

    # get detailed info from one guild
    def get_one_guild(self, guild):
        """Get one guild setting from guilds collection by guild"""

        # guild_c = GuildsRnDaoModel(
        #     self.DB_connection.mongoOps.mongo_db_access.db_mongo_client["RnDAO"]
        # )

        # result = guild_c.get_guild_info(guild)
        result = self.DB_connection.mongoOps.mongo_db_access.db_mongo_client["RnDAO"][
            "guilds"
        ].find_one({"guildId": guild})
        return result

    # get all user accounts during date_range in guild from rawinfo data
    def get_all_users(
        self,
        guildId: str,
    ) -> list[str]:
        # check guild is exist

        client = self.DB_connection.mongoOps.mongo_db_access.db_mongo_client

        if guildId not in client.list_database_names():
            logging.error(f"Database {guildId} doesn't exist")
            logging.error(f"Existing databases: {client.list_database_names()}")
            logging.info("Continuing")
            return []

        cursor = client[guildId]["guildmembers"].find(
            {
                "isBot": {"$ne": True},
            },
            {"discordId": 1, "_id": 0},
        )

        users_data = list(cursor)
        all_users = list(map(lambda x: x["discordId"], users_data))

        return all_users

    def parse_reaction(self, s):
        result = []
        for subitem in s:
            items = subitem.split(",")
            parsed_items = []
            for item in items:
                parsed_items.append(item)
            self.merge_array(result, result[:-1])
        return result

    def get_users_from_oneday(self, entries):
        """get all users from one day messages"""
        users = []
        for entry in entries:
            # author
            if entry["author"]:
                self.merge_array(users, [entry["author"]])
            # mentioned users
            # mentions = entry["user_mentions"][0].split(",")
            mentions = entry["user_mentions"]
            self.merge_array(users, mentions)
            # reacters
            reactions = self.parse_reaction(entry["reactions"])
            self.merge_array(users, reactions)
        return users

    def merge_array(self, parent_arr, child_arr):
        """insert all elements in child_arr to parent_arr which are not in parent_arr"""
        for element in child_arr:
            if element == "":
                continue
            if element not in parent_arr:
                parent_arr.append(element)
