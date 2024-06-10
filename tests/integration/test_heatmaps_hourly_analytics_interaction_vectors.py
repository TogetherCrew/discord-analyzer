from unittest import TestCase

from datetime import datetime
from discord_analyzer.analyzer.heatmaps.analytics_hourly import AnalyticsHourly
from utils.mongo import MongoSingleton


class TestHeatmapsRawAnalyticsVectorsInteractions(TestCase):
    """
    test the 24 hour vector 
    """
    def setUp(self) -> None:
        client = MongoSingleton.get_instance().get_client()
        platform_id = "781298"
        database = client[platform_id]
        database.drop_collection("rawmemberactivities")
        self.database = database

        self.analytics = AnalyticsHourly(platform_id)

    def test_empty_data(self):
        day = datetime(2023, 1, 1)
        activity_vector = self.analytics.analyze(
            day,
            activity="interactions",
            activity_name="reply",
            author_id=9000,
            activity_type="emitter",
            additional_filters={"metadata.channel_id": 123},
        )

        self.assertIsInstance(activity_vector, list)
        self.assertEqual(len(activity_vector), 24)
        self.assertEqual(sum(activity_vector), 0)

    def test_no_relevant_data(self):
        day = datetime(2023, 1, 1)

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
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 6),
                "source_id": "10001",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.database["rawmemberactivities"].insert_many(sample_raw_data)

        activity_vector = self.analytics.analyze(
            day,
            activity="interactions",
            activity_name="reply",
            author_id=9000,
            activity_type="emitter",
            additional_filters={"metadata.channel_id": 2000},
        )

        self.assertIsInstance(activity_vector, list)
        self.assertEqual(len(activity_vector), 24)
        self.assertEqual(sum(activity_vector), 0)

    def test_single_relevant_data_type_receiver(self):
        day = datetime(2023, 1, 1)

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
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": day,
                "source_id": "10001",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
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
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 4),
                "source_id": "10001",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.database["rawmemberactivities"].insert_many(sample_raw_data)

        activity_vector = self.analytics.analyze(
            day,
            activity="interactions",
            activity_name="reply",
            author_id=9000,
            activity_type="receiver",
            additional_filters={"metadata.channel_id": 2000},
        )

        self.assertIsInstance(activity_vector, list)
        self.assertEqual(len(activity_vector), 24)
        self.assertEqual(
            activity_vector,
            [
                0,
                0,
                1,
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
            ],
        )

    def test_single_relevant_data(self):
        day = datetime(2023, 1, 1)

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
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": day,
                "source_id": "10001",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.database["rawmemberactivities"].insert_many(sample_raw_data)

        activity_vector = self.analytics.analyze(
            day,
            activity="interactions",
            activity_name="reply",
            author_id=9000,
            activity_type="emitter",
            additional_filters={
                "metadata.channel_id": 2000,
            },
        )

        self.assertIsInstance(activity_vector, list)
        self.assertEqual(len(activity_vector), 24)
        self.assertEqual(
            activity_vector,
            [
                0,
                0,
                1,
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
            ],
        )

    def test_multiple_relevant_data(self):
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
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
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 5),
                "source_id": "10001",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    },
                    {"name": "reply", "users_engaged_id": [9003], "type": "emitter"},
                ],
            },
        ]
        self.database["rawmemberactivities"].insert_many(sample_raw_data)

        activity_vector = self.analytics.analyze(
            day,
            author_id=9001,
            activity="interactions",
            activity_name="reply",
            activity_type="emitter",
            additional_filters={"metadata.channel_id": 2000},
        )

        self.assertIsInstance(activity_vector, list)
        self.assertEqual(len(activity_vector), 24)
        self.assertEqual(
            activity_vector,
            [
                0,
                0,
                1,
                0,
                0,
                1,
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
            ],
        )

    def test_replier_wrong_activity_type(self):
        day = datetime(2023, 1, 1)

        with self.assertRaises(ValueError):
            self.analytics.analyze(
            activity="interactions",
            activity_name="reply",
                day=day,
                author_id=9000,
                activity_type="wrong_type",
            )

    def test_replier_wrong_activity(self):
        day = datetime(2023, 1, 1)

        with self.assertRaises(ValueError):
            self.analytics.analyze(
            activity="activity1",
            activity_name="reply",
                day=day,
                author_id=9000,
                activity_type="emitter",
            )