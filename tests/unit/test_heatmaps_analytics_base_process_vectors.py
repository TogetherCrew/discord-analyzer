from unittest import TestCase

from tc_analyzer_lib.metrics.heatmaps.analytics_hourly import AnalyticsHourly


class TestRawMemberActivitiesProcessVectors(TestCase):
    def setUp(self) -> None:
        self.platform_id = "3456789"
        self.analytics_hourly = AnalyticsHourly(self.platform_id)

    def test_no_input(self):
        input_data = []
        hourly_analytics = self.analytics_hourly._process_vectors(input_data)
        self.assertIsInstance(hourly_analytics, list)

        # zeros vector with length 24
        expected_analytics = [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        self.assertEqual(len(hourly_analytics), 24)
        self.assertEqual(hourly_analytics, expected_analytics)

    def test_single_input(self):
        # hour 0 of the day had an activity of 2
        input_data = [{"_id": 0, "count": 2}]
        hourly_analytics = self.analytics_hourly._process_vectors(input_data)
        self.assertIsInstance(hourly_analytics, list)

        expected_analytics = [
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        self.assertEqual(len(hourly_analytics), 24)
        self.assertEqual(hourly_analytics, expected_analytics)

    def test_multiple_input(self):
        # hour 0 of the day had an activity of 2
        input_data = [
            {"_id": 0, "count": 2},
            {"_id": 3, "count": 4},
            {"_id": 19, "count": 7},
        ]
        hourly_analytics = self.analytics_hourly._process_vectors(input_data)
        self.assertIsInstance(hourly_analytics, list)

        expected_analytics = [
            2,
            0,
            0,
            4,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            7,
            0,
            0,
            0,
            0,
        ]
        self.assertEqual(len(hourly_analytics), 24)
        self.assertEqual(hourly_analytics, expected_analytics)
