from tc_analyzer_lib.schemas import HourlyAnalytics, RawAnalytics


class PlatformConfigBase:
    def __init__(
        self,
        platform: str,
        resource_identifier: str,
        hourly_analytics: list[HourlyAnalytics],
        raw_analytics: list[RawAnalytics],
    ):
        self.platform = platform
        self.resource_identifier = resource_identifier
        self.hourly_analytics = hourly_analytics
        self.raw_analytics = raw_analytics

    def to_dict(self):
        return {
            "platform": self.platform,
            "resource_identifier": self.resource_identifier,
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
            resource_identifier=data["resource_identifier"],
            hourly_analytics=hourly_analytics,
            raw_analytics=raw_analytics,
        )
