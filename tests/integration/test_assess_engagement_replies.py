from datetime import datetime, timedelta
from unittest import TestCase

import pytest
from discord_analyzer.algorithms.utils.member_activity_utils import assess_engagement
from discord_analyzer.metrics.utils.analyzer_db_manager import AnalyzerDBManager
from tc_core_analyzer_lib.utils.activity import DiscordActivity
from utils.credentials import get_mongo_credentials

from .utils.analyzer_setup import launch_db_access
from .utils.remove_and_setup_guild import setup_db_guild


@pytest.mark.skip("Skipping for now as memberactivities is not updated!")
class TestAssessEngagementReplies(TestCase):
    def setUp(self) -> None:
        self.guildId = "1234"
        self.db_access = launch_db_access(self.guildId)
        self.create_db_connections()

    def create_db_connections(self):
        base_analyzer = AnalyzerDBManager()
        mongo_creds = get_mongo_credentials()
        base_analyzer.set_mongo_database_info(
            mongo_db_user=mongo_creds["user"],
            mongo_db_password=mongo_creds["password"],
            mongo_db_host=mongo_creds["host"],
            mongo_db_port=mongo_creds["port"],
        )
        base_analyzer.database_connect()
        self.db_connections = base_analyzer.DB_connections

    def heatmaps_analytics(self):
        """
        heatmaps are the input for assess_engagement's interaction matrix
        """
        from discord_analyzer.metrics.heatmaps import Heatmaps

        heatmaps = Heatmaps(DB_connections=self.db_connections, testing=False)
        heatmaps_data = heatmaps.analysis_heatmap(guildId=self.guildId, from_start=True)
        analytics_data = {}
        analytics_data["heatmaps"] = heatmaps_data
        analytics_data["memberactivities"] = (None, None)
        self.db_connections.store_analytics_data(
            guild_id=self.guildId,
            analytics_data=analytics_data,
            community_id="123",
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
        platform_id = "515151515151515151515151"
        setup_db_guild(
            self.db_access,
            platform_id,
            self.guildId,
            discordId_list=users_id_list,
            days_ago_period=35,
            action=action,
        )
        self.db_access.db_mongo_client[self.guildId]["heatmaps"].delete_many({})
        self.db_access.db_mongo_client[self.guildId].create_collection("heatmaps")

        rawinfo_samples = []
        analyze_dates = set()
        for i in range(35 * 24):
            raw_data_date = datetime.now() - timedelta(hours=i)
            sample = {
                "type": 19,
                "author": "user1",
                "content": f"test message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": "user2",
                "createdDate": raw_data_date,
                "messageId": f"11188143219343360{i}",
                "channelId": "1020707129214111827",
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            analyze_dates.add(raw_data_date.strftime("%Y-%m-%d"))
            rawinfo_samples.append(sample)

        self.db_access.db_mongo_client[self.guildId]["rawinfos"].insert_many(
            rawinfo_samples
        )
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
            action_params=action,
            period_size=7,
            db_access=self.db_access,
            channels=["1020707129214111827"],
            analyze_dates=list(analyze_dates),
            activities_name=list(activity_dict.keys()),
            activity_dict=activity_dict,
            activities_to_analyze=[
                DiscordActivity.Reply,
            ],
        )
        self.assertEqual(activity_dict["all_active"], {"0": set(["user1"])})
