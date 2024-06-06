from unittest import TestCase

from discord_analyzer.schemas import ActivityType, HourlyAnalytics


class TestHourlyAnalytics(TestCase):

    def test_initialization_with_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.ACTION,
            member_activities_used=True,
            metadata_condition={"key": "value"},
        )
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertTrue(analytics.member_activities_used)
        self.assertEqual(analytics.metadata_condition, {"key": "value"})

    def test_initialization_without_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.INTERACTION,
            member_activities_used=True,
            metadata_condition=None,
        )
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.INTERACTION)
        self.assertTrue(analytics.member_activities_used)
        self.assertIsNone(analytics.metadata_condition)

    def test_to_dict_with_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.ACTION,
            member_activities_used=True,
            metadata_condition={"key": "value"},
        )
        expected_dict = {
            "name": "analytics1",
            "type": "action",
            "member_activities_used": True,
            "metadata_condition": {"key": "value"},
        }
        self.assertEqual(analytics.to_dict(), expected_dict)

    def test_to_dict_without_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.INTERACTION,
            member_activities_used=True,
        )
        expected_dict = {
            "name": "analytics1",
            "type": "interaction",
            "member_activities_used": True,
        }
        self.assertEqual(analytics.to_dict(), expected_dict)

    def test_from_dict_with_metadata(self):
        data = {
            "name": "analytics1",
            "type": "action",
            "member_activities_used": True,
            "metadata_condition": {"key": "value"},
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertTrue(analytics.member_activities_used)
        self.assertEqual(analytics.metadata_condition, {"key": "value"})

    def test_from_dict_without_metadata(self):
        data = {
            "name": "analytics1",
            "type": "interaction",
            "member_activities_used": True,
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.INTERACTION)
        self.assertTrue(analytics.member_activities_used)
        self.assertIsNone(analytics.metadata_condition)
