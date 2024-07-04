from unittest import TestCase

from tc_analyzer_lib.schemas import ActivityDirection, ActivityType
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig


class TestDiscordAnalyzerConfig(TestCase):
    def test_discord_schema_overview(self):
        # checking the analyzer schema for discord platform
        config = DiscordAnalyzerConfig()
        self.assertEqual(config.platform, "discord")
        self.assertEqual(config.resource_identifier, "channel_id")

        # we have 8 hourly analytics
        self.assertEqual(len(config.hourly_analytics), 8)

        # we have 3 raw analytics
        self.assertEqual(len(config.raw_analytics), 3)

    def test_discord_schema_hourly_analytics(self):
        hourly_analytics = DiscordAnalyzerConfig().hourly_analytics
        for anlaytics in hourly_analytics:
            if anlaytics.name == "thr_messages":
                self.assertEqual(anlaytics.type, ActivityType.ACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.EMITTER)
                self.assertEqual(anlaytics.member_activities_used, True)
                self.assertEqual(
                    anlaytics.rawmemberactivities_condition,
                    {"metadata.thread_id": {"$ne": None}},
                )
            elif anlaytics.name == "lone_messages":
                self.assertEqual(anlaytics.type, ActivityType.ACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.EMITTER)
                self.assertEqual(anlaytics.member_activities_used, True)
                self.assertEqual(
                    anlaytics.rawmemberactivities_condition,
                    {"metadata.thread_id": None},
                )
            elif anlaytics.name == "replier":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.RECEIVER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            elif anlaytics.name == "replied":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.EMITTER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            elif anlaytics.name == "mentioner":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.EMITTER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            elif anlaytics.name == "mentioned":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.RECEIVER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            elif anlaytics.name == "reacter":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.RECEIVER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            elif anlaytics.name == "reacted":
                self.assertEqual(anlaytics.type, ActivityType.INTERACTION)
                self.assertEqual(anlaytics.direction, ActivityDirection.EMITTER)
                self.assertEqual(anlaytics.member_activities_used, False)
                self.assertIsNone(anlaytics.rawmemberactivities_condition)
            else:
                raise ValueError("No more hourly analytics for discord be available!")

    def test_discord_schema_raw_analytics(self):
        raw_analytics = DiscordAnalyzerConfig().raw_analytics
        for analytics in raw_analytics:
            if analytics.name == "replied_per_acc":
                self.assertTrue(analytics.member_activities_used)
                self.assertEqual(analytics.type, ActivityType.INTERACTION)
                self.assertEqual(analytics.direction, ActivityDirection.EMITTER)
            elif analytics.name == "mentioner_per_acc":
                self.assertTrue(analytics.member_activities_used)
                self.assertEqual(analytics.type, ActivityType.INTERACTION)
                self.assertEqual(analytics.direction, ActivityDirection.EMITTER)
            elif analytics.name == "reacted_per_acc":
                self.assertTrue(analytics.member_activities_used)
                self.assertEqual(analytics.type, ActivityType.INTERACTION)
            else:
                raise ValueError(
                    "No more raw analytics for discord should be available!"
                )
