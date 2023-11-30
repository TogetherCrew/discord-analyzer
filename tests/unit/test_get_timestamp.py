import unittest
from datetime import datetime, timezone

from discord_analyzer.DB_operations.network_graph import get_timestamp


class TestGetTimestamp(unittest.TestCase):
    def test_current_time(self):
        """
        Test when no time is provided, it should return the current timestamp.
        """
        result = get_timestamp()
        current_time = (
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
            * 1000
        )
        self.assertAlmostEqual(result, current_time, delta=1000)

    def test_specific_time(self):
        """
        Test when a specific time is provided, it should return the correct timestamp.
        """
        specific_time = datetime(2023, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        result = get_timestamp(specific_time)
        expected_timestamp = (
            specific_time.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            * 1000
        )
        self.assertAlmostEqual(result, expected_timestamp, delta=1000)

    def test_none_input(self):
        """
        Test when `None` is provided as input, it should behave the same as not providing any time.
        """
        result_with_none = get_timestamp(None)
        result_without_none = get_timestamp()

        self.assertAlmostEqual(result_with_none, result_without_none, delta=1000)

    def test_past_time(self):
        """
        Test when a past time is provided, it should return the correct timestamp.
        """
        past_time = datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = get_timestamp(past_time)
        expected_timestamp = (
            past_time.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            * 1000
        )
        self.assertAlmostEqual(result, expected_timestamp, delta=1000)

    def test_microseconds(self):
        """
        Test when the provided time has microseconds, it should handle them correctly.
        """
        time_with_microseconds = datetime(
            2023, 1, 1, 12, 30, 0, 500000, tzinfo=timezone.utc
        )
        result = get_timestamp(time_with_microseconds)
        expected_timestamp = (
            time_with_microseconds.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp()
            * 1000
        )
        self.assertAlmostEqual(result, expected_timestamp, delta=1000)
