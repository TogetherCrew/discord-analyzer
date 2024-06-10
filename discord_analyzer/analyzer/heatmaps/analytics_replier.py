from datetime import datetime

from discord_analyzer.analyzer.heatmaps.analytics_base import AnalyticsBase


class AnalyticsReplier(AnalyticsBase):
    def __init__(self, platform_id: str) -> None:
        super().__init__(platform_id)

    def analyze(
        self,
        day: datetime.date,
        author_id: int,
        type: str,
        additional_filters: dict[str, str],
    ) -> list[int]:
        """
        analyze the `replier` meaning who replied to a message

        Parameters
        ------------
        day : datetime.date
            analyze for a specific day
        author_id : str
            the author to filter data for
        additional_filters : dict[str, str]
            the additional filtering for rawmemberactivities data of each platform
            the keys could be `metadata.channel_id` with a specific value
        type : str
            should be always either `emitter` or `receiver`
        """
        if type not in ["emitter", "receiver"]:
            raise ValueError(
                "Wrong type given, should be either `emitter` or `receiver`!"
            )
        replier = self.get_hourly_analytics(
            day=day,
            activity="interactions",
            author_id=author_id,
            filters={
                "interactions.name": "reply",
                "interactions.type": type,
                **additional_filters,
            },
        )

        return replier
