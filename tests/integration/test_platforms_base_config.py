import unittest

from tc_analyzer_lib.schemas import (
    ActivityDirection,
    ActivityType,
    HourlyAnalytics,
    RawAnalytics,
)
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase


class TestPlatformBaseConfig(unittest.TestCase):
    def test_config_to_dict(self):
        analytics = HourlyAnalytics(
            name="thr_messages",
            type=ActivityType.ACTION,
            member_activities_used=False,
            direction=ActivityDirection.RECEIVER,
            rawmemberactivities_condition={"thread_id": {"$ne": None}},
            activity_name="actions",
        )
        expected_dict = {
            "name": "thr_messages",
            "type": "actions",
            "member_activities_used": False,
            "direction": "receiver",
            "rawmemberactivities_condition": {"thread_id": {"$ne": None}},
            "activity_name": "actions",
        }
        self.assertEqual(analytics.to_dict(), expected_dict)

    def test_analytics_from_dict(self):
        data = {
            "name": "thr_messages",
            "type": "actions",
            "member_activities_used": False,
            "direction": "emitter",
            "rawmemberactivities_condition": {"thread_id": {"$ne": None}},
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "thr_messages")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertFalse(analytics.member_activities_used)
        self.assertEqual(
            analytics.rawmemberactivities_condition, {"thread_id": {"$ne": None}}
        )

    def test_analytics_data_to_dict(self):
        hourly_analytics = [
            HourlyAnalytics(
                name="thr_messages",
                type=ActivityType.ACTION,
                member_activities_used=False,
                direction=ActivityDirection.EMITTER,
                rawmemberactivities_condition={"thread_id": {"$ne": None}},
            )
        ]
        raw_analytics = [
            RawAnalytics(
                name="replied_per_acc",
                type=ActivityType.INTERACTION,
                member_activities_used=True,
                direction=ActivityDirection.RECEIVER,
            )
        ]
        analytics_data = PlatformConfigBase(
            platform="discord",
            resource_identifier="channel_id",
            hourly_analytics=hourly_analytics,
            raw_analytics=raw_analytics,
        )
        expected_dict = {
            "platform": "discord",
            "resource_identifier": "channel_id",
            "hourly_analytics": [analytic.to_dict() for analytic in hourly_analytics],
            "raw_analytics": [analytic.to_dict() for analytic in raw_analytics],
        }
        self.assertEqual(analytics_data.to_dict(), expected_dict)

    def test_analytics_data_from_dict(self):
        data = {
            "platform": "discord",
            "resource_identifier": "chat_id",
            "hourly_analytics": [
                {
                    "name": "thr_messages",
                    "type": "actions",
                    "member_activities_used": False,
                    "rawmemberactivities_condition": {"thread_id": {"$ne": None}},
                    "direction": "emitter",
                }
            ],
            "raw_analytics": [
                {
                    "name": "replied_per_acc",
                    "type": "interactions",
                    "member_activities_used": True,
                    "direction": "receiver",
                }
            ],
        }
        analyzer_config = PlatformConfigBase.from_dict(data)
        self.assertEqual(analyzer_config.platform, "discord")
        self.assertEqual(analyzer_config.resource_identifier, "chat_id")
        self.assertEqual(len(analyzer_config.hourly_analytics), 1)
        self.assertEqual(analyzer_config.hourly_analytics[0].name, "thr_messages")
        self.assertEqual(analyzer_config.hourly_analytics[0].type, ActivityType.ACTION)
        self.assertEqual(
            analyzer_config.hourly_analytics[0].direction, ActivityDirection.EMITTER
        )
        self.assertFalse(analyzer_config.hourly_analytics[0].member_activities_used)
        self.assertEqual(
            analyzer_config.hourly_analytics[0].rawmemberactivities_condition,
            {"thread_id": {"$ne": None}},
        )
        self.assertEqual(len(analyzer_config.raw_analytics), 1)
        self.assertEqual(analyzer_config.raw_analytics[0].name, "replied_per_acc")
        self.assertEqual(
            analyzer_config.raw_analytics[0].type, ActivityType.INTERACTION
        )
        self.assertEqual(
            analyzer_config.raw_analytics[0].direction, ActivityDirection.RECEIVER
        )
        self.assertTrue(analyzer_config.raw_analytics[0].member_activities_used)
