import logging

from dateutil import parser
from tc_analyzer_lib.utils.mongo import MongoSingleton


class MemberActivityUtils:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

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
            if first_date is None or activity["date"] > first_date:
                data_to_save.append(activity)

        return data_to_save

    # get detailed info from one guild
    def get_one_guild(self, guild):
        """Get one guild setting from guilds collection by guild"""

        result = self.client["Core"]["platforms"].find_one({"metadata.id": guild})
        return result

    # get all user accounts during date_range in guild from rawinfo data
    def get_all_users(
        self,
        guildId: str,
    ) -> list[str]:
        all_users: list[str]

        # check guild is exist
        if guildId not in self.client.list_database_names():
            logging.error(
                f"Database {guildId} doesn't exist! Returning empty array for users"
            )
            all_users = []
        else:
            cursor = self.client[guildId]["rawmembers"].find(
                {
                    "is_bot": {"$ne": True},
                },
                {"id": 1, "_id": 0},
            )
            users_data = list(cursor)
            all_users = list(map(lambda x: x["id"], users_data))

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
