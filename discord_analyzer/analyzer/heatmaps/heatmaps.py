import logging
from datetime import datetime, timedelta

from discord_analyzer.analyzer.heatmaps.heatmaps_utils import HeatmapsUtils
from discord_analyzer.analyzer.heatmaps import AnalyticsHourly, AnalyticsRaw
from utils.mongo import MongoSingleton
from discord_analyzer.models.HeatMapModel import HeatMapModel
from discord_analyzer.schemas import RawAnalyticsItem
from discord_analyzer.schemas.platform_configs.config_base import PlatformConfigBase


class Heatmaps:
    def __init__(
        self,
        platform_id: str,
        period: datetime,
        resources: list[str],
        analyzer_config: PlatformConfigBase,
    ) -> None:
        """
        Heatmaps analytics wrapper

        Parameters
        ------------
        platform_id : str
            the platform that we want heatmaps analytics for
        period : datetime
            the date that analytics could be started
        resources : list[str]
            a list of resources id
            i.e. a list of `channel_id` for discord or `chat_id` for telegram
        analyzer_config : PlatformConfigBase
            the configuration for analytics job
            should be a class inheriting from `PlatformConfigBase` and with predefined values
        """
        self.platform_id = platform_id
        self.resources = resources
        self.client = MongoSingleton.get_instance().get_client()
        self.period = period

        self.analyzer_config = analyzer_config
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
        log_prefix = f"PLATFORMID: {self.platform_id}:"

        heatmap_c = HeatMapModel(self.client[self.platform_id])
        last_date = heatmap_c.get_last_date()

        analytics_date: datetime
        if last_date is None or from_start:
            analytics_date = self.period
        else:
            analytics_date = last_date + timedelta(days=1)

        # initialize the data array
        heatmaps_results = []

        # using mongodb cursor for efficient data retrieval
        user_ids_cursor = self.utils.get_users()
        users_count = self.utils.get_users_count()

        iteration_count = self._compute_iteration_counts(
            analytics_date=analytics_date,
            resources_count=len(self.resources),
            authors_count=users_count,
        )

        index = 0
        while analytics_date.date() < datetime.now().date():

            for resource_id in self.resources:

                for author in user_ids_cursor:

                    logging.info(
                        f"{log_prefix} ANALYZING HEATMAPS {index}/{iteration_count}"
                    )
                    index += 1

                    author_id = author["id"]
                    document = {
                        self.analyzer_config.resource_identifier: resource_id,
                        "date": analytics_date,
                        "user": author_id,
                    }
                    hourly_analytics = self._process_hourly_analytics(
                        day=analytics_date,
                        resource=resource_id,
                        author_id=author_id,
                        hourly_analytics_config=self.analyzer_config.hourly_analytics,
                    )

                    document["hourly_analytics"] = hourly_analytics

                    raw_analytics = self._process_raw_analytics(
                        day=analytics_date,
                        resource=resource_id,
                        author_id=author_id,
                        raw_analytics_config=self.analyzer_config.raw_analytics,
                    )
                    document["raw_analytics"] = raw_analytics

                    heatmaps_results.append(document)

            # analyze next day
            analytics_date += timedelta(days=1)

        return heatmaps_results

    def _process_hourly_analytics(
        self,
        day: datetime.date,
        resource: str,
        author_id: str,
    ) -> dict[str, list]:
        """
        start processing hourly analytics for a day based on given config

        Parameters
        ------------
        day : datetime.date
            analyze for a specific day
        resurce : str
            the resource we want to apply the filtering on
        author_id : str
            the author to filter data for
        """
        analytics_hourly = AnalyticsHourly(self.platform_id)
        analytics: dict[str, list[int]] = {}
        for config in self.analyzer_config.hourly_analytics:

            # if it was a predefined analytics
            if config.name in [
                "replied",
                "replier",
                "mentioner",
                "mentioned",
                "reacter",
                "reacted",
            ]:
                activity_name: str
                if config.name in ["replied", "replier"]:
                    activity_name = "reply"
                elif config.name in ["mentioner", "mentioned"]:
                    activity_name = "mention"
                else:
                    activity_name = "reaction"

                analytics_vector = analytics_hourly.analyze(
                    day=day,
                    activity=config.type.value,
                    activity_name=activity_name,
                    activity_direction=config.direction.value,
                    author_id=author_id,
                    additional_filters={
                        f"metadata.{self.analyzer_config.resource_identifier}": resource,
                    },
                )
                analytics[config.name] = analytics_vector

            # if it was a custom analytics that we didn't write code
            # the mongodb condition is given in their configuration
            else:
                conditions = config.rawmemberactivities_condition
                activity_name = config.activity_name

                if activity_name is None:
                    raise ValueError(
                        "For custom analytics the `activity_name` "
                        "in analyzer config shouldn't be None"
                    )

                analytics_vector = analytics_hourly.analyze(
                    day=day,
                    activity=config.type.value,
                    activity_name=activity_name,
                    activity_direction=config.direction.value,
                    author_id=author_id,
                    additional_filters={
                        f"metadata.{self.analyzer_config.resource_identifier}": resource,
                        **conditions,
                    },
                )
                analytics[config.name] = analytics_vector

        return analytics

    def _process_raw_analytics(
        self,
        day: datetime.date,
        resource: str,
        author_id: str,
    ) -> dict[str, list[RawAnalyticsItem]]:
        analytics_raw = AnalyticsRaw(self.platform_id)
        analytics: dict[str, list[RawAnalyticsItem]] = {}

        for config in self.analyzer_config.raw_analytics:

            # default analytics that we always can have
            activity_name: str
            if config.name == "reacted_per_acc":
                activity_name = "reaction"
            elif config.name == "mentioner_per_acc":
                activity_name = "mention"
            elif config.name == "replied_per_acc":
                activity_name = "reply"
            else:
                # custom analytics
                if config.activity_name is None:
                    raise ValueError(
                        "`activity_name` for custom analytics should be provided"
                    )
                activity_name = config.activity_name

            additional_filters: dict[str, str] = {
                f"metadata.{self.analyzer_config.resource_identifier}": resource,
            }
            # preparing for custom analytics (if available in config)
            if config.rawmemberactivities_condition is not None:
                additional_filters = {
                    **additional_filters,
                    **config.rawmemberactivities_condition,
                }

            analytics_items = analytics_raw.analyze(
                day=day,
                activity=config.type.value,
                activity_name=activity_name,
                activity_direction=config.direction.value,
                author_id=author_id,
                additional_filters=additional_filters,
            )

            analytics[config.name] = analytics_items

        return analytics

    def _compute_iteration_counts(
        self,
        analytics_date: datetime,
        resources_count: int,
        authors_count: int,
    ) -> int:
        iteration_count = (
            (datetime.now() - analytics_date).days * resources_count * authors_count
        )

        return iteration_count
