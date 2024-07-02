import logging
from datetime import datetime, timedelta

from tc_analyzer_lib.algorithms.compute_member_activity import compute_member_activity
from tc_analyzer_lib.metrics.memberactivity_utils import MemberActivityUtils
from tc_analyzer_lib.models.MemberActivityModel import MemberActivityModel
from tc_analyzer_lib.models.RawInfoModel import RawInfoModel
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase
from tc_analyzer_lib.utils.mongo import MongoSingleton


class MemberActivities:
    def __init__(
        self,
        platform_id: str,
        resources: list[str],
        action_config: dict[str, int],
        window_config: dict[str, int],
        analyzer_config: PlatformConfigBase,
        analyzer_period: datetime,
    ) -> None:
        self.platform_id = platform_id
        self.resources = resources
        self.action_config = action_config
        self.window_config = window_config
        self.analyzer_config = analyzer_config
        self.analyzer_period = analyzer_period
        self.utils = MemberActivityUtils()

    def analysis_member_activity(
        self, from_start: bool = False
    ) -> tuple[list[dict], list]:
        """
        Based on the rawdata creates and stores the member activity data

        Parameters:
        -------------
        from_start : bool
            do the analytics from scrach or not
            if True, if wouldn't pay attention to the existing data in memberactivities
            and will do the analysis from the first date

        Returns:
        ---------
        memberactivity_results : list of dictionary
            the list of data analyzed
            also the return could be None if no database for platform
              or no raw info data was available
        memberactivity_networkx_results : list of networkx objects
            the list of data analyzed in networkx format
            also the return could be None if no database for platform
             or no raw info data was available
        """
        guild_msg = f"PLATFORMID: {self.platform_id}:"

        client = MongoSingleton.get_instance().get_client()

        # check current platform is exist
        if self.platform_id not in client.list_database_names():
            logging.error(f"{guild_msg} Database {self.platform_id} doesn't exist")
            logging.error(f"{guild_msg} No such databse!")
            logging.info(f"{guild_msg} Continuing")
            return (None, None)

        member_activity_c = MemberActivityModel(client[self.platform_id])
        rawinfo_c = RawInfoModel(client[self.platform_id])

        # Testing if there are entries in the rawinfo collection
        if rawinfo_c.count() == 0:
            logging.warning(
                f"No entries in the collection 'rawmemberactivities' in {self.platform_id} databse"
            )
            return (None, None)

        # get date range to be analyzed
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        logging.info(f"{guild_msg} memberactivities Analysis started!")

        # initialize
        load_past_data = False

        # if we had data from past to use
        if member_activity_c.count() != 0:
            load_past_data = True

        load_past_data = load_past_data and not from_start

        first_date = self.analyzer_period.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if first_date is None:
            logging.error(
                f"No platform: {self.platform_id} available in platforms.core!"
            )
            return None, None

        last_date = today - timedelta(days=1)

        date_range: list[datetime] = [first_date, last_date]

        if load_past_data:
            period_size = self.window_config["period_size"]
            num_days_to_load = (
                max(
                    [
                        self.action_config["CON_T_THR"],
                        self.action_config["VITAL_T_THR"],
                        self.action_config["STILL_T_THR"],
                        self.action_config["PAUSED_T_THR"],
                    ]
                )
                + 1
            ) * period_size
            date_range[0] = date_range[1] - timedelta(days=num_days_to_load)

            # if the date range goes back more than the "7 days `period` forward"
            if date_range[0] < self.analyzer_period + timedelta(days=period_size):
                date_range[0] = self.analyzer_period + timedelta(days=period_size)

        # get all users during date_range
        all_users = self.utils.get_all_users(self.platform_id)

        networkx_objects, activities = compute_member_activity(
            platform_id=self.platform_id,
            resources=self.resources,
            resource_identifier=self.analyzer_config.resource_identifier,
            acc_names=all_users,
            date_range=date_range,
            window_param=self.window_config,
            act_param=self.action_config,
            load_past_data=load_past_data,
            analyzer_config=self.analyzer_config,
        )

        if not from_start:
            # first date of storing the data
            first_storing_date = member_activity_c.get_last_date()
            activities = self.utils.refine_memberactivities_data(
                activities, first_storing_date
            )

        memberactivity_results = activities
        memberactivity_networkx_results = networkx_objects

        return memberactivity_results, memberactivity_networkx_results
