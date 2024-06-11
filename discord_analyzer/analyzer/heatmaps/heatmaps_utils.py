from pymongo.cursor import Cursor

from discord_analyzer.schemas.accounts import AccountCounts
from utils.mongo import MongoSingleton


class HeatmapsUtils:
    def __init__(self, platform_id: str) -> None:
        self.platform_id = platform_id
        client = MongoSingleton.get_instance().get_client()
        self.database = client[platform_id]

    def get_users(self, is_bot: bool = False) -> Cursor:
        """
        get the users of a platform

        Parameters
        -----------
        is_bot : bool
            if we want to fetch the bots
            for default is False meaning the real users will be returned

        Returns:
        ---------
        bots : pymongo.cursor.Cursor
            MongoDB cursor for users
            in case of large amount of data we should loop over this
            the cursor data format would be as `{'id': xxxx}`
        """
        cursor = self.database["rawmembers"].find(
            {"is_bot": is_bot}, {"_id": 0, "id": 1}
        )
        return cursor
    
    def get_users_count(self, is_bot: bool = False) -> int:
        """
        get the count of users

        Parameters
        -----------
        is_bot : bool
            if we want to fetch the bots
            for default is False meaning the real users will be returned

        Returns
        ---------
        users_count : int
            the count of users
        """
        users_count = self.database["rawmembers"].count_documents(
            {"is_bot": is_bot},
        )
        return users_count

    def store_counts_dict(self, counts_dict):
        # make empty result array
        obj_array = []

        # for each account
        for acc in counts_dict.keys():
            # make dict and store in array
            obj_array.append(AccountCounts(acc, counts_dict[acc]).todict())

        return obj_array
