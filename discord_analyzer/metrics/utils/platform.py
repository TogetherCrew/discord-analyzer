from bson import ObjectId
from datetime import datetime

from utils.mongo import MongoSingleton


class Platform:
    def __init__(self, platform_id: str) -> None:
        """
        the utilities for platform

        Parameters
        ------------
        platform_id : str
            a specific platform's id
        """
        self.platform_id = platform_id
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
            {"_id": ObjectId(self.platform_id)},
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
        existance = self.check_existance()
        if existance is False:
            raise AttributeError("No such a platform available!")

        self.client["Core"]["platforms"].update_one(
            {"_id": ObjectId(self.platform_id)},
            {"$set": {"metadata.isInProgress": False}},
        )

    def get_community_id(self) -> str:
        """
        get the community id of a platform

        Returns
        --------
        community_id : str
            the community that the Guild is related to
        """
        platform = self.client["Core"]["platforms"].find_one(
            {"_id": ObjectId(self.platform_id)}, {"community": 1}
        )
        if platform is None:
            raise ValueError(
                f"No platform is available for the given platform: {self.platform_id}"
            )

        community_id = str(platform["community"])

        return community_id

    def get_platform_period(self) -> datetime:
        """
        get the period field for analyzer of a platform

        Returns
        --------
        period : datetime
            the period which the analyzer should start its work from
        """
        platform = self.client["Core"]["platforms"].find_one(
            {"_id": ObjectId(self.platform_id)},
            {"metadata.period": 1},
        )

        if platform is None:
            raise AttributeError(
                f"No such platform for platform_id: {self.platform_id}"
            )

        period = platform["metadata"]["period"]
        return period

    def get_platform_resources(self) -> list[str]:
        """
        get the platform resources id
        This will do the initial filtering on data

        Returns
        ---------
        resources : list[str]
            a list of resources to do filtering on data
        """
        platform = self.client["Core"]["platforms"].find_one(
            {"_id": ObjectId(self.platform_id)},
            {"metadata.resources": 1},
        )

        if platform is None:
            raise AttributeError(
                f"No such platform for platform_id: {self.platform_id}"
            )

        resources = platform["metadata"]["resources"]
        return resources