from unittest import TestCase

from tc_analyzer_lib.schemas import ActivityDirection, ActivityType, HourlyAnalytics


class TestHourlyAnalytics(TestCase):
    def test_initialization_with_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.ACTION,
            member_activities_used=True,
            rawmemberactivities_condition={"key": "value"},
            direction=ActivityDirection.EMITTER,
        )
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertTrue(analytics.member_activities_used)
        self.assertEqual(analytics.rawmemberactivities_condition, {"key": "value"})
        self.assertEqual(analytics.direction, ActivityDirection.EMITTER)

    def test_initialization_without_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.INTERACTION,
            member_activities_used=True,
            rawmemberactivities_condition=None,
            direction=ActivityDirection.RECEIVER,
        )
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.INTERACTION)
        self.assertEqual(analytics.direction, ActivityDirection.RECEIVER)
        self.assertTrue(analytics.member_activities_used)
        self.assertIsNone(analytics.rawmemberactivities_condition)

    def test_to_dict_with_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.ACTION,
            member_activities_used=True,
            rawmemberactivities_condition={"key": "value"},
            direction=ActivityDirection.EMITTER,
        )
        expected_dict = {
            "name": "analytics1",
            "type": "actions",
            "member_activities_used": True,
            "rawmemberactivities_condition": {"key": "value"},
            "direction": "emitter",
            "activity_name": None,
        }
        self.assertEqual(analytics.to_dict(), expected_dict)

    def test_to_dict_without_metadata(self):
        analytics = HourlyAnalytics(
            name="analytics1",
            type=ActivityType.INTERACTION,
            member_activities_used=True,
            direction=ActivityDirection.RECEIVER,
        )
        expected_dict = {
            "name": "analytics1",
            "type": "interactions",
            "member_activities_used": True,
            "direction": "receiver",
            "activity_name": None,
        }
        self.assertEqual(analytics.to_dict(), expected_dict)

    def test_from_dict_with_metadata(self):
        data = {
            "name": "analytics1",
            "type": "actions",
            "member_activities_used": True,
            "rawmemberactivities_condition": {"key": "value"},
            "direction": "emitter",
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.ACTION)
        self.assertEqual(analytics.direction, ActivityDirection.EMITTER)
        self.assertTrue(analytics.member_activities_used)
        self.assertEqual(analytics.rawmemberactivities_condition, {"key": "value"})

    def test_from_dict_without_metadata(self):
        data = {
            "name": "analytics1",
            "type": "interactions",
            "member_activities_used": True,
            "direction": "receiver",
        }
        analytics = HourlyAnalytics.from_dict(data)
        self.assertEqual(analytics.name, "analytics1")
        self.assertEqual(analytics.type, ActivityType.INTERACTION)
        self.assertEqual(analytics.direction, ActivityDirection.RECEIVER)
        self.assertTrue(analytics.member_activities_used)
        self.assertIsNone(analytics.rawmemberactivities_condition)
