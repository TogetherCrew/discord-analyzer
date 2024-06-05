from datetime import datetime
from .hourly_analytics import HourlyAnalytics
from .raw_analytics import RawAnalytics


class HeatmapsConfig:
    def __init__(
        self,
        platform: str,
        date: datetime.date,
        channel_id: str,
        user_id: str,
        hourly_analytics: list[HourlyAnalytics],
        raw_analytics: list[RawAnalytics],
    ):
        self.platform = platform
        self.date = date
        self.channel_id = channel_id
        self.user_id = user_id
        self.hourly_analytics = hourly_analytics
        self.raw_analytics = raw_analytics

    def to_dict(self):
        return {
            "platform": self.platform,
            "date": self.date,
            "channel_id": self.channel_id,
            "user_id": self.user_id,
            "hourly_analytics": [ha.to_dict() for ha in self.hourly_analytics],
            "raw_analytics": [ra.to_dict() for ra in self.raw_analytics],
        }

    @classmethod
    def from_dict(cls, data: dict):
        hourly_analytics = [
            HourlyAnalytics.from_dict(ha) for ha in data["hourly_analytics"]
        ]
        raw_analytics = [RawAnalytics.from_dict(ra) for ra in data["raw_analytics"]]
        return cls(
            platform=data["platform"],
            date=data["date"],
            channel_id=data["channel_id"],
            user_id=data["user_id"],
            hourly_analytics=hourly_analytics,
            raw_analytics=raw_analytics,
        )
