from discord_analyzer.schemas.accounts import AccountCounts
from utils.mongo import MongoSingleton


class HeatmapsUtils:
    def __init__(self, platform_id: str) -> None:
        self.platform_id = platform_id
        self.client = MongoSingleton.get_instance().get_client()

    def get_users(self, is_bot: bool = False) -> list[str]:
        """
        get the users of a platform

        Parameters
        -----------
        is_bot : bool
            if we want to fetch the bots
            for default is False meaning the real users will be returned

        Returns:
        ---------
        bots : list[str]
            the list of bot ids
        """
        cursor = self.client[self.platform_id]["rawmembers"].find(
            {"is_bot": is_bot}, {"_id": 0, "id": 1}
        )
        bots = list(cursor)

        bot_ids = []
        if bots != []:
            bot_ids = list(map(lambda x: x["id"], bots))

        return bot_ids

    def store_counts_dict(self, counts_dict):
        # make empty result array
        obj_array = []

        # for each account
        for acc in counts_dict.keys():
            # make dict and store in array
            obj_array.append(AccountCounts(acc, counts_dict[acc]).todict())

        return obj_array

    def getNumberOfActions(self, heatmap):
        """get number of actions"""
        sum_ac = 0
        fields = [
            "thr_messages",
            "lone_messages",
            "replier",
            "replied",
            "mentioned",
            "mentioner",
            "reacter",
            "reacted",
        ]
        for field in fields:
            for i in range(24):
                sum_ac += heatmap[field][i]
        return sum_ac
