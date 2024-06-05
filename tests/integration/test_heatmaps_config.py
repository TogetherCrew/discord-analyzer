import unittest
from datetime import date

from discord_analyzer.schemas import ActivityType, HourlyAnalytics, RawAnalytics, HeatmapsConfig

class TestAnalyticsModels(unittest.TestCase):
    
    def test_analytics_to_dict(self):
        analytics = HourlyAnalytics(
            name="thr_messages",
            type=ActivityType.ACTION,
            member_activities_used=False,
            metadata_condition={"threadId": {"$ne": None}}
        )
        expected_dict = {
            "name": "thr_messages",
            "type": "action",
            "member_activities_used": False,
            "metadata_condition": {"threadId": {"$ne": None}}
        }
        self.assertEqual(analytics.to_dict(), expected_dict)
    
    def test_analytics_from_dict(self):
        data = {
            "name": "thr_messages",
            "type": "action",
            "member_activities_used": False,
            "metadata_condition": {"threadId": {"$ne": None}}
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "thr_messages")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertFalse(analytics.member_activities_used)
        self.assertEqual(analytics.metadata_condition, {"threadId": {"$ne": None}})
    
    def test_analytics_data_to_dict(self):
        hourly_analytics = [
            HourlyAnalytics(
                name="thr_messages",
                type=ActivityType.ACTION,
                member_activities_used=False,
                metadata_condition={"threadId": {"$ne": None}}
            )
        ]
        raw_analytics = [
            RawAnalytics(
                name="replied_per_acc",
                type=ActivityType.INTERACTION,
                member_activities_used=True
            )
        ]
        analytics_data = HeatmapsConfig(
            platform="discord",
            date=date(2023, 6, 5),
            channel_id="channel_123",
            user_id="user_456",
            hourly_analytics=hourly_analytics,
            raw_analytics=raw_analytics
        )
        expected_dict = {
            "platform": "discord",
            "date": date(2023, 6, 5),
            "channel_id": "channel_123",
            "user_id": "user_456",
            "hourly_analytics": [analytic.to_dict() for analytic in hourly_analytics],
            "raw_analytics": [analytic.to_dict() for analytic in raw_analytics]
        }
        self.assertEqual(analytics_data.to_dict(), expected_dict)
    
    def test_analytics_data_from_dict(self):
        data = {
            "platform": "discord",
            "date": date(2023, 6, 5),
            "channel_id": "channel_123",
            "user_id": "user_456",
            "hourly_analytics": [
                {
                    "name": "thr_messages",
                    "type": "action",
                    "member_activities_used": False,
                    "metadata_condition": {"threadId": {"$ne": None}}
                }
            ],
            "raw_analytics": [
                {
                    "name": "replied_per_acc",
                    "type": "interaction",
                    "member_activities_used": True
                }
            ]
        }
        heatmaps_config = HeatmapsConfig.from_dict(data)
        self.assertEqual(heatmaps_config.platform, "discord")
        self.assertEqual(heatmaps_config.date, date(2023, 6, 5))
        self.assertEqual(heatmaps_config.channel_id, "channel_123")
        self.assertEqual(heatmaps_config.user_id, "user_456")
        self.assertEqual(len(heatmaps_config.hourly_analytics), 1)
        self.assertEqual(heatmaps_config.hourly_analytics[0].name, "thr_messages")
        self.assertEqual(heatmaps_config.hourly_analytics[0].type, ActivityType.ACTION)
        self.assertFalse(heatmaps_config.hourly_analytics[0].member_activities_used)
        self.assertEqual(heatmaps_config.hourly_analytics[0].metadata_condition, {"threadId": {"$ne": None}})
        self.assertEqual(len(heatmaps_config.raw_analytics), 1)
        self.assertEqual(heatmaps_config.raw_analytics[0].name, "replied_per_acc")
        self.assertEqual(heatmaps_config.raw_analytics[0].type, ActivityType.INTERACTION)
        self.assertTrue(heatmaps_config.raw_analytics[0].member_activities_used)