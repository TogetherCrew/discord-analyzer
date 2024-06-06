from discord_analyzer.schemas import ActivityType, HourlyAnalytics, RawAnalytics
from discord_analyzer.schemas.platform_configs.config_base import PlatformConfigBase


class DiscordAnalyzerConfig(PlatformConfigBase):
    def __init__(self):
        platform: str = "discord"
        resource_identifier: str = "channel_id"
        hourly_analytics: list[HourlyAnalytics] = [
            HourlyAnalytics(
                name="thr_messages",
                type=ActivityType.ACTION,
                member_activities_used=True,
                metadata_condition={
                    "threadId": {"$ne": None},
                }
            ),
            HourlyAnalytics(
                name="lone_messages",
                type=ActivityType.ACTION,
                member_activities_used=True,
                metadata_condition={
                    "threadId": None,
                }
            ),
            HourlyAnalytics(
                name="replier",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
            HourlyAnalytics(
                name="replied",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
            HourlyAnalytics(
                name="mentioner",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
            HourlyAnalytics(
                name="mentioned",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
            HourlyAnalytics(
                name="reacter",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
            HourlyAnalytics(
                name="reacted",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
            ),
        ]

        raw_analytics: list[RawAnalytics] = [
                RawAnalytics(
                    name="replied_per_acc",
                    type=ActivityType.INTERACTION,
                    member_activities_used=True
                ),
                RawAnalytics(
                    name="mentioner_per_acc",
                    type=ActivityType.INTERACTION,
                    member_activities_used=True
                ),
                RawAnalytics(
                    name="reacted_per_acc",
                    type=ActivityType.INTERACTION,
                    member_activities_used=True
                ),
        ]
        
        super().__init__(platform, resource_identifier, hourly_analytics, raw_analytics)
