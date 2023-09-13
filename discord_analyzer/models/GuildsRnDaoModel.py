#!/usr/bin/env python3
import logging

from discord_analyzer.models.BaseModel import BaseModel


class GuildsRnDaoModel(BaseModel):
    def __init__(self, database=None):
        if database is None:
            logging.exception("Database does not exist.")
            raise Exception("Database should not be None")
        super().__init__(collection_name="guilds", database=database)
        # print(self.database[self.collection_name].find_one())

    def get_connected_guilds(self, guildId):
        """
        Returns the list of the connected guilds if guildId is None
        Otherwise the list of one connected guild with given guildId
        """
        findOption = {"isDisconnected": False}
        if guildId is not None:
            findOption["guildId"] = guildId
        guilds = self.database[self.collection_name].find(findOption)
        return [x["guildId"] for x in guilds]

    def get_guild_info(self, guildId):
        """
        Return detailed information of guild settings
        Return None if such guild is not exist
        """
        guild = self.database[self.collection_name].find({"guildId": guildId})
        if guild == []:
            return None
        return guild[0]

    def get_guild_period(self, guildId: str):
        """
        get the period field from guild saved in RnDAO collection
        """
        data = self.database[self.collection_name].find_one(
            {"guildId": guildId}, {"period": 1, "_id": 0}
        )
        if data is not None:
            return data["period"]
        else:
            return None

    def get_guild_channels(self, guildId):
        """
        get the channelSelection from a guild

        Parameters:
        -------------
        guildId : str
            the guildId to update its channel selection


        Returns:
        ----------
        channels : list of dictionaries
            a list of dictionaries representative of channelName, channelId, and _id
        """

        query = {"guildId": f"{guildId}"}
        feature_projection = {"selectedChannels": 1, "_id": 0}

        cursor = self.database[self.collection_name].find(
            query, projection=feature_projection
        )

        channels = list(cursor)

        # initialize
        selected_channels = None

        if channels == []:
            selected_channels = []
        else:
            selected_channels = channels[0]["selectedChannels"]

        return selected_channels

    def update_guild_channel_selection(self, guildId, selectedChannels):
        """
        Update the channel selection in RnDAO for a specific guild

        Parameters:
        ------------
        guildId : str
            the guildId to update its channel selection
        selectedChannels : dict
            a dictionary for the channel selection,
             each key values of dictionary must have the followings
               `channelId`, `channelName`, and `_id`
            example:
            {'0': {
                'channelId': '1073334445554337223',
                'channelName': 'sample_channel_name',
                '_id': ObjectId('156a84sd1')
            },
            '1': {...}
            }

        Returns:
        -----------
        status : bool
            if True, the channel selection is updated
            else, the channel selection is not updated
        """
        # query to filter the guilds of the RnDAO.guilds table
        query = {"guildId": f"{guildId}"}

        update_field_query = {"$set": {"selectedChannels": selectedChannels}}

        # update the selectedChannels
        self.database[self.collection_name].update_one(query, update_field_query)
