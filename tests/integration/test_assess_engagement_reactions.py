from datetime import datetime, timedelta
from unittest import TestCase

from .utils.analyzer_setup import launch_db_access
from .utils.remove_and_setup_guild import setup_db_guild
from discord_analyzer.analysis.utils.member_activity_utils import (
    assess_engagement,
)
from discord_analyzer.analyzer.analyzer_heatmaps import Heatmaps
from tc_core_analyzer_lib.utils.activity import DiscordActivity
from discord_analyzer.analyzer.base_analyzer import Base_analyzer
from utils.daolytics_uitls import (
    get_mongo_credentials,
    get_neo4j_credentials,
)


class TestAssessEngagementReactions(TestCase):
    def setUp(self) -> None:
        self.guildId = "1234"
        self.db_access = launch_db_access(self.guildId)
        self.create_db_connections()

    def create_db_connections(self):
        base_analyzer = Base_analyzer()
        mongo_creds = get_mongo_credentials()
        base_analyzer.set_mongo_database_info(
            mongo_db_user=mongo_creds["user"],
            mongo_db_password=mongo_creds["password"],
            mongo_db_host=mongo_creds["host"],
            mongo_db_port=mongo_creds["port"],
        )
        neo4j_creds = get_neo4j_credentials()
        base_analyzer.set_neo4j_database_info(neo4j_creds)
        base_analyzer.database_connect()
        self.db_connections = base_analyzer.DB_connections

    def heatmaps_analytics(self):
        """
        heatmaps are the input for assess_engagement's interaction matrix
        """
        heatmaps = Heatmaps(DB_connections=self.db_connections, testing=False)
        heatmaps_data = heatmaps.analysis_heatmap(guildId=self.guildId, from_start=True)
        analytics_data = {}
        analytics_data[f"{self.guildId}"] = {
            "heatmaps": heatmaps_data,
            "memberactivities": (
                None,
                None,
            ),
        }
        self.db_connections.store_analytics_data(
            analytics_data=analytics_data,
            community_id="123",
            remove_memberactivities=False,
            remove_heatmaps=False,
        )

    def test_single_user_reaction(self):
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
        setup_db_guild(
            self.db_access,
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
                "type": 0,
                "author": "user1",
                "content": f"test message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": ["user2,👍"],
                "replied_user": None,
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
                DiscordActivity.Reaction,
            ],
        )
        self.assertEqual(activity_dict["all_active"], {"0": set(["user2"])})
