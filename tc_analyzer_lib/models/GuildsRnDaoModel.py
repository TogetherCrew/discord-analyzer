from pymongo.database import Database
from tc_analyzer_lib.models.BaseModel import BaseModel


class GuildsRnDaoModel(BaseModel):
    def __init__(self, database: Database):
        super().__init__(collection_name="platforms", database=database)
        # print(self.database[self.collection_name].find_one())

    def get_connected_guilds(self, guildId: str | None = None):
        """
        Returns the list of the connected guilds if guildId is None
        Otherwise the list of one connected guild with given guildId
        """
        findOption = {"disconnectedAt": None, "name": "discord"}
        if guildId is not None:
            findOption["metadata.id"] = guildId

        guilds = self.database[self.collection_name].find(findOption)
        return [x["metadata"]["id"] for x in guilds]

    def get_guild_info(self, guildId):
        """
        Return detailed information of guild settings
        Return None if such guild is not exist
        """
        guild = self.database[self.collection_name].find_one({"metadata.id": guildId})
        return guild

    def get_guild_period(self, guildId: str):
        """
        get the period field from guild saved in platforms collection
        """
        data = self.database[self.collection_name].find_one(
            {"metadata.id": guildId, "name": "discord"},
            {"metadata.period": 1, "_id": 0},
        )
        if data is not None:
            return data["metadata"]["period"]
        else:
            return None

    def get_guild_channels(self, guildId: str) -> list[dict]:
        """
        get the channelSelection from a guild

        Parameters:
        -------------
        guildId : str
            the guildId to update its channel selection


        Returns:
        ----------
        channels : list[dict]
            a list of dictionaries representative of channelName, channelId, and _id
        """

        query = {"metadata.id": guildId, "name": "discord"}
        feature_projection = {"metadata.selectedChannels": 1, "_id": 0}

        results = self.database[self.collection_name].find_one(
            query, projection=feature_projection
        )

        selected_channels: list[dict]
        if results is None:
            selected_channels = []
        else:
            selected_channels = results["metadata"]["selectedChannels"]

        return selected_channels
