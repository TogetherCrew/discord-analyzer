from datetime import datetime
from unittest import TestCase

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsProcessHourlySingleDay(TestCase):
    def setUp(self) -> None:
        platform_id = "1234567890"
        period = datetime(2024, 1, 1)
        resources = list["123", "124", "125"]
        # using one of the configs we currently have
        # it could be any other platform's config
        discord_analyzer_config = DiscordAnalyzerConfig()

        self.heatmaps = Heatmaps(
            platform_id=platform_id,
            period=period,
            resources=resources,
            analyzer_config=discord_analyzer_config,
        )
        self.mongo_client = MongoSingleton.get_instance().get_client()
        self.mongo_client[platform_id].drop_collection("rawmemberactivities")
        self.mongo_client[platform_id].drop_collection("heatmaps")

    def test_process_hourly_check_return_type(self):
        day = datetime(2023, 1, 1)

        hourly_analytics = self.heatmaps._process_hourly_analytics(
            day,
            resource="124",
            author_id=9001,
        )

        self.assertIsInstance(hourly_analytics, dict)
        # the config was discord analyzer
        self.assertIn("replied", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["replied"], list)
        self.assertEqual(len(hourly_analytics["replied"]), 24)

        self.assertIn("replier", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["replier"], list)
        self.assertEqual(len(hourly_analytics["replier"]), 24)

        self.assertIn("mentioned", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["mentioned"], list)
        self.assertEqual(len(hourly_analytics["mentioned"]), 24)

        self.assertIn("mentioner", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["mentioner"], list)
        self.assertEqual(len(hourly_analytics["mentioner"]), 24)

        self.assertIn("reacter", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["reacter"], list)
        self.assertEqual(len(hourly_analytics["reacter"]), 24)

        self.assertIn("reacted", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["reacted"], list)
        self.assertEqual(len(hourly_analytics["reacted"]), 24)

        self.assertIn("thr_messages", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["thr_messages"], list)
        self.assertEqual(len(hourly_analytics["thr_messages"]), 24)

        self.assertIn("lone_messages", hourly_analytics.keys())
        self.assertIsInstance(hourly_analytics["lone_messages"], list)
        self.assertEqual(len(hourly_analytics["lone_messages"]), 24)

    def test_process_hourly_single_author(self):
        platform_id = self.heatmaps.platform_id
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": None, "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        hourly_analytics = self.heatmaps._process_hourly_analytics(
            day,
            resource="124",
            author_id=9001,
        )

        self.assertEqual(hourly_analytics["mentioner"][0], 2)
        self.assertEqual(hourly_analytics["mentioner"][4], 2)
        self.assertEqual(sum(hourly_analytics["mentioner"]), 4)
        self.assertEqual(sum(hourly_analytics["mentioned"]), 0)
        self.assertEqual(sum(hourly_analytics["reacter"]), 0)
        self.assertEqual(sum(hourly_analytics["reacted"]), 0)

        self.assertEqual(hourly_analytics["replied"][2], 1)
        self.assertEqual(sum(hourly_analytics["replied"]), 1)

        self.assertEqual(hourly_analytics["replier"][2], 1)
        self.assertEqual(sum(hourly_analytics["replier"]), 1)

        self.assertEqual(hourly_analytics["thr_messages"][0], 1)
        self.assertEqual(hourly_analytics["thr_messages"][2], 2)
        self.assertEqual(sum(hourly_analytics["thr_messages"]), 3)

        self.assertEqual(hourly_analytics["lone_messages"][4], 1)
        self.assertEqual(sum(hourly_analytics["lone_messages"]), 1)

    def test_process_hourly_wrong_channel(self):
        """
        running the process hourly for another channel that no data is available for
        """
        platform_id = self.heatmaps.platform_id
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": None, "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        hourly_analytics = self.heatmaps._process_hourly_analytics(
            day,
            resource="125",
            author_id=9001,
        )

        self.assertEqual(sum(hourly_analytics["mentioned"]), 0)
        self.assertEqual(sum(hourly_analytics["mentioner"]), 0)
        self.assertEqual(sum(hourly_analytics["reacter"]), 0)
        self.assertEqual(sum(hourly_analytics["reacted"]), 0)
        self.assertEqual(sum(hourly_analytics["replied"]), 0)
        self.assertEqual(sum(hourly_analytics["replier"]), 0)
        self.assertEqual(sum(hourly_analytics["thr_messages"]), 0)
        self.assertEqual(sum(hourly_analytics["lone_messages"]), 0)

    def test_process_hourly_wrong_author(self):
        """
        running the process hourly for another author that no data is available for
        """
        platform_id = self.heatmaps.platform_id
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": "7000", "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
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
                "metadata": {"thread_id": None, "channel_id": "124"},
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        hourly_analytics = self.heatmaps._process_hourly_analytics(
            day,
            resource="124",
            author_id=9005,
        )

        self.assertEqual(sum(hourly_analytics["mentioned"]), 0)
        self.assertEqual(sum(hourly_analytics["mentioner"]), 0)
        self.assertEqual(sum(hourly_analytics["reacter"]), 0)
        self.assertEqual(sum(hourly_analytics["reacted"]), 0)
        self.assertEqual(sum(hourly_analytics["replied"]), 0)
        self.assertEqual(sum(hourly_analytics["replier"]), 0)
        self.assertEqual(sum(hourly_analytics["thr_messages"]), 0)
        self.assertEqual(sum(hourly_analytics["lone_messages"]), 0)
