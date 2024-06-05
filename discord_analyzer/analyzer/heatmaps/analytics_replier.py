from datetime import datetime

from discord_analyzer.analyzer.heatmaps.analytics_base import AnalyticsBase


class AnalyticsReplier(AnalyticsBase):
    def __init__(self, platform_id: str) -> None:
        super().__init__(platform_id)

    def analyze(
        self,
        day: datetime.date,
    ) -> list[int]:
        """
        analyze the `replier` meaning who replied to a message

        Parameters
        ------------
        day : datetime.date
            analyze for a specific day
        msg : str
            additional information to be logged
            for default is empty string meaning no additional string to log
        """
        replier = self.get_hourly_analytics(
            day=day,
            activity="interactions",
            filters={
                "interactions.name": "reply",
                "interactions.type": "emitter",
            },
        )

        return replier
