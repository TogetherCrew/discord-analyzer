from datetime import datetime
from unittest import TestCase

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig


class TestAnalyzerHeatmapsIterationCount(TestCase):
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

    def test_compute_iteration_counts(self):
        analytics_date = datetime(2024, 1, 1)
        now = datetime.now()

        days = (now - analytics_date).days

        iteration_count = self.heatmaps._compute_iteration_counts(
            analytics_date=analytics_date,
            resources_count=5,
            authors_count=5,
        )

        self.assertEqual(iteration_count, days * 5 * 5)  # five days
