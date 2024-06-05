import logging
from collections import Counter
from datetime import datetime, timedelta

from discord_analyzer.analysis.activity_hourly import activity_hourly
from discord_analyzer.analyzer.heatmaps.heatmaps_utils import HeatmapsUtils
from discord_analyzer.analyzer.heatmaps import AnalyticsReplier
from utils.mongo import MongoSingleton
from discord_analyzer.models.GuildsRnDaoModel import GuildsRnDaoModel
from discord_analyzer.models.HeatMapModel import HeatMapModel
from discord_analyzer.models.RawInfoModel import RawInfoModel


class Heatmaps:
    def __init__(
            self,
            platform_id: str,
            period: datetime,
            heatmaps_config: dict
        ) -> None:
        """
        Heatmaps analytics wrapper

        Parameters
        ------------
        platform_id : str
            the platform that we want heatmaps analytics for
        heatmaps_config : dict
            the dictionary representing what analytics for heatmaps would be computed
        """
        self.platform_id = platform_id
        self.heatmaps_config = heatmaps_config
        self.client = MongoSingleton.get_instance().get_client()
        self.period = period

        self.utils = HeatmapsUtils(platform_id)

    def start(self, from_start: bool = False):
        """
        Based on the rawdata creates and stores the heatmap data

        Parameters:
        -------------
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
        guild_msg = f"PLATFORMID: {self.platform_id}:"

        # Collections involved in analysis
        # guild parameter is the name of the database
        rawinfo_c = RawInfoModel(self.client[self.platform_id])
        heatmap_c = HeatMapModel(self.client[self.platform_id])

        # Testing if there are entries in the rawinfo collection
        if rawinfo_c.count() == 0:
            msg = f"{guild_msg} No entries in the collection"
            msg += "'rawinfos' in {guildId} databse"
            logging.warning(msg)
            return None

        last_date = heatmap_c.get_last_date()

        if last_date is None or from_start:
            last_date = self.period
        else:
            last_date = last_date + timedelta(days=1)

        analyzer_replier = AnalyticsReplier(self.platform_id)

        # initialize the data array
        heatmaps_results = []

        bot_ids = self.utils.get_users(is_bot=True)

        while last_date.date() < datetime.now().date():
            entries = rawinfo_c.get_day_entries(last_date, "ANALYZER HEATMAPS: ")
            if len(entries) == 0:
                # analyze next day
                last_date = last_date + timedelta(days=1)
                continue

            prepared_list = []
            
            # Too aggresive
            # TODO: filter for the day users on raw data
            users = self.utils.get_users(is_bot=False)
            analyzer_replier.analyze


            # TODO: update
            results = []
            heatmaps_results.extend(results)

            # analyze next day
            last_date = last_date + timedelta(days=1)

        return heatmaps_results
    
    def _process_hourly_analytics(
            self, day: datetime.date, hourly_analytics_config: dict
        ) -> dict[str, list]:
        """
        start processing hourly analytics for a day based on given config
        """
        
        pass

    def _process_raw_analytics(
            self, day: datetime.date, raw_analytics_config: dict
        ) -> dict[str, list]:
        
        pass
        

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
