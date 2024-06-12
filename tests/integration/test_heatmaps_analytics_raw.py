from unittest import TestCase
from datetime import datetime, timedelta

from discord_analyzer.analyzer.heatmaps.analytics_raw import AnalyticsRaw
from utils.mongo import MongoSingleton
from discord_analyzer.schemas import ActivityDirection, RawAnalyticsItem


class TestHeatmapsRawAnalytics(TestCase):
    def setUp(self) -> None:
        self.platform_id = "3456789"
        self.analytics_raw = AnalyticsRaw(self.platform_id)
        self.mongo_client = MongoSingleton.get_instance().get_client()
        self.mongo_client[self.platform_id].drop_collection("rawmemberactivities")

    def test_raw_analytics_single_user(self):
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            author_id=9000,
        )

        self.assertIsInstance(analytics_result, RawAnalyticsItem)
        self.assertEqual(analytics_result.account, 9000)
        self.assertEqual(analytics_result.count, 1)

    def test_raw_analytics_wrong_user(self):
        """
        asking for another user's analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            author_id=9003,
        )

        self.assertIsNone(analytics_result)

    def test_raw_analytics_wrong_activity_direction(self):
        """
        asking for another activity direction analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.EMITTER.value,
            author_id=9000,
        )

        self.assertIsNone(analytics_result)

    def test_raw_analytics_wrong_day(self):
        """
        asking for another day analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day + timedelta(days=1),
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            author_id=9000,
        )
        self.assertIsNone(analytics_result)

    def test_raw_analytics_wrong_activity(self):
        """
        asking for another activity analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="mention",
            activity_direction=ActivityDirection.RECEIVER.value,
            author_id=9000,
        )

        self.assertIsNone(analytics_result)

    def test_raw_analytics_multiple_users(self):
        """
        asking for another activity analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 4),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [9006, 9005],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 2, 4),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9001,
                        ],
                        "type": "receiver",
                    }
                ],
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            author_id=9000,
        )

        self.assertIsInstance(analytics_result, RawAnalyticsItem)

        self.assertEqual(analytics_result.account, 9000)
        self.assertEqual(analytics_result.count, 2)
