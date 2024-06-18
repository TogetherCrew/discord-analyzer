from unittest import TestCase

from discord_analyzer.schemas import ActivityType, ActivityDirection
from discord_analyzer.schemas.platform_configs.config_base import RawAnalytics


class TestRawAnalytics(TestCase):
    def test_initialization(self):
        # Valid initialization
        raw_analytics = RawAnalytics(
            name="analytics1",
            type=ActivityType.ACTION,
            member_activities_used=True,
            direction=ActivityDirection.EMITTER,
        )
        self.assertEqual(raw_analytics.name, "analytics1")
        self.assertEqual(raw_analytics.type, ActivityType.ACTION)
        self.assertTrue(raw_analytics.member_activities_used)

        # Invalid initialization (Invalid ActivityType)
        with self.assertRaises(ValueError):
            RawAnalytics(
                name="analytics1",
                type="invalid_type",
                member_activities_used=True,
                direction=ActivityDirection.RECEIVER,
            )

    def test_to_dict(self):
        raw_analytics = RawAnalytics(
            name="analytics1",
            type=ActivityType.INTERACTION,
            member_activities_used=False,
            direction=ActivityDirection.EMITTER,
        )
        expected_dict = {
            "name": "analytics1",
            "type": "interaction",
            "member_activities_used": False,
        }
        self.assertEqual(raw_analytics.to_dict(), expected_dict)

    def test_from_dict(self):
        data = {
            "name": "analytics1",
            "type": "action",
            "member_activities_used": True,
        }
        raw_analytics = RawAnalytics.from_dict(data)
        self.assertEqual(raw_analytics.name, "analytics1")
        self.assertEqual(raw_analytics.type, ActivityType.ACTION)
        self.assertTrue(raw_analytics.member_activities_used)

        # Invalid from_dict (missing keys)
        invalid_data = {
            "name": "analytics1",
            "member_activities_used": True,
        }
        with self.assertRaises(KeyError):
            RawAnalytics.from_dict(invalid_data)

        # Invalid from_dict (invalid type)
        invalid_data_type = {
            "name": "analytics1",
            "type": "invalid_type",
            "member_activities_used": True,
        }
        with self.assertRaises(ValueError):
            RawAnalytics.from_dict(invalid_data_type)
