from datetime import datetime, timedelta
from unittest import TestCase

from tc_analyzer_lib.algorithms.utils.member_activity_utils import assess_engagement
from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.metrics.utils.analyzer_db_manager import AnalyzerDBManager
from tc_analyzer_lib.schemas import GraphSchema
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


class TestAssessEngagementReplies(TestCase):
    def setUp(self) -> None:
        platform_id = "515151515151515151515151"
        self.db_access = launch_db_access(platform_id)
        self.create_db_connections()

        period = datetime(2024, 1, 1)
        resources = ["123", "124", "125"]
        # using one of the configs we currently have
        # it could be any other platform's config
        discord_analyzer_config = DiscordAnalyzerConfig()

        self.heatmaps = Heatmaps(
            platform_id=platform_id,
            period=period,
            resources=resources,
            analyzer_config=discord_analyzer_config,
        )

    def create_db_connections(self):
        base_analyzer = AnalyzerDBManager()
        base_analyzer.database_connect()
        self.db_connections = base_analyzer.DB_connections

    def heatmaps_analytics(self):
        """
        heatmaps are the input for assess_engagement's interaction matrix
        """
        heatmaps_data = self.heatmaps.start(from_start=True)

        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)
        grpah_schema = GraphSchema(platform=self.heatmaps.analyzer_config.platform)
        self.db_connections.store_analytics_data(
            platform_id=self.heatmaps.platform_id,
            analytics_data=analytics_data,
            graph_schema=grpah_schema,
            remove_memberactivities=False,
            remove_heatmaps=False,
        )

    def test_single_user_reply(self):
        """
        just actions and no interaction
        """
        users_id_list = ["user1", "user2"]
        action = {
            "INT_THR": 1,
            "UW_DEG_THR": 1,
            "PAUSED_T_THR": 1,
            "CON_T_THR": 4,
            "CON_O_THR": 3,
            "EDGE_STR_THR": 5,
            "UW_THR_DEG_THR": 5,
            "VITAL_T_THR": 4,
            "VITAL_O_THR": 3,
            "STILL_T_THR": 2,
            "STILL_O_THR": 2,
            "DROP_H_THR": 2,
            "DROP_I_THR": 1,
        }
        setup_platform(
            self.db_access,
            self.heatmaps.platform_id,
            discordId_list=users_id_list,
            days_ago_period=35,
            action=action,
        )
        self.db_access.db_mongo_client[self.heatmaps.platform_id].drop_collection(
            "heatmaps"
        )
        self.db_access.db_mongo_client[self.heatmaps.platform_id].drop_collection(
            "rawmemberactivities"
        )

        rawinfo_samples = []
        analyze_dates = [datetime.now() - timedelta(hours=35 * 24), datetime.now()]
        for i in range(35 * 24):
            raw_data_date = datetime.now() - timedelta(hours=i)
            author = "user1"
            replied_user = "user2"
            samples = [
                {
                    "actions": [{"name": "message", "type": "emitter"}],
                    "author_id": author,
                    "date": raw_data_date,
                    "interactions": [
                        {
                            "name": "reply",
                            "type": "emitter",
                            "users_engaged_id": [replied_user],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "123",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
                {
                    "actions": [],
                    "author_id": replied_user,
                    "date": raw_data_date,
                    "interactions": [
                        {
                            "name": "reply",
                            "type": "receiver",
                            "users_engaged_id": [author],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "123",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
            ]
            rawinfo_samples.extend(samples)

        self.db_access.db_mongo_client[self.heatmaps.platform_id][
            "rawmemberactivities"
        ].insert_many(rawinfo_samples)
        self.heatmaps_analytics()

        activity_dict: dict[str, dict] = {
            "all_joined": {"0": set()},
            "all_joined_day": {"0": set()},
            "all_consistent": {},
            "all_vital": {},
            "all_active": {},
            "all_connected": {},
            "all_paused": {},
            "all_new_disengaged": {},
            "all_disengaged": {},
            "all_unpaused": {},
            "all_returned": {},
            "all_new_active": {},
            "all_still_active": {},
            "all_dropped": {},
            "all_disengaged_were_newly_active": {},
            "all_disengaged_were_consistently_active": {},
            "all_disengaged_were_vital": {},
            "all_lurker": {},
            "all_about_to_disengage": {},
            "all_disengaged_in_past": {},
        }
        _, activity_dict = assess_engagement(
            w_i=0,
            accounts=users_id_list,
            platform_id=self.heatmaps.platform_id,
            action_params=action,
            period_size=7,
            resources=["123"],
            resource_identifier="channel_id",
            analyze_dates=analyze_dates,
            activities_name=list(activity_dict.keys()),
            activity_dict=activity_dict,
            analyzer_config=self.heatmaps.analyzer_config,
        )
        self.assertEqual(activity_dict["all_active"], {"0": set(["user1"])})
