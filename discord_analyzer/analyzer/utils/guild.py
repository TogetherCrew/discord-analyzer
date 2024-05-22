from utils.mongo import MongoSingleton


class Guild:
    # TODO: Update to `Platform` and add `platform_id` in future
    def __init__(self, guild_id: str) -> None:
        self.guild_id = guild_id
        self.client = MongoSingleton.get_instance().get_client()

    def check_existance(self) -> bool:
        """
        check for existance of a Guild

        Returns
        ----------
        exists : bool
            if the Guild exist or not
        """
        platform = self.client["Core"]["platforms"].find_one(
            {"metadata.id": self.guild_id},
            {"_id": 1},
        )
        exists: bool
        if platform is None:
            exists = False
        else:
            exists = True

        return exists

    def update_isin_progress(self):
        """
        update isInProgress field of platforms collection
        """
        self.client["Core"]["platforms"].update_one(
            {"metadata.id": self.guild_id}, {"$set": {"metadata.isInProgress": False}}
        )

    def get_community_id(self) -> str:
        """
        get the community id of a guild

        Returns
        --------
        community_id : str
            the community that the Guild is related to
        """
        platform = self.client["Core"]["platforms"].find_one(
            {"metadata.id": self.guild_id}, {"community": 1}
        )
        if platform is None:
            raise ValueError(
                f"No platform is available for the given guild: {self.guild_id}"
            )

        community_id = str(platform["community"])

        return community_id
