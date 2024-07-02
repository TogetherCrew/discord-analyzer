from tc_analyzer_lib.schemas import (
    ActivityDirection,
    ActivityType,
    HourlyAnalytics,
    RawAnalytics,
)
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase


class DiscordAnalyzerConfig(PlatformConfigBase):
    def __init__(self):
        platform: str = "discord"
        resource_identifier: str = "channel_id"
        hourly_analytics: list[HourlyAnalytics] = [
            HourlyAnalytics(
                name="thr_messages",
                type=ActivityType.ACTION,
                member_activities_used=True,
                rawmemberactivities_condition={
                    "metadata.thread_id": {"$ne": None},
                },
                direction=ActivityDirection.EMITTER,
                activity_name="message",
            ),
            HourlyAnalytics(
                name="lone_messages",
                type=ActivityType.ACTION,
                member_activities_used=True,
                rawmemberactivities_condition={
                    "metadata.thread_id": None,
                },
                direction=ActivityDirection.EMITTER,
                activity_name="message",
            ),
            HourlyAnalytics(
                name="replier",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.RECEIVER,
            ),
            HourlyAnalytics(
                name="replied",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.EMITTER,
            ),
            HourlyAnalytics(
                name="mentioner",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.EMITTER,
            ),
            HourlyAnalytics(
                name="mentioned",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.RECEIVER,
            ),
            HourlyAnalytics(
                name="reacter",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.RECEIVER,
            ),
            HourlyAnalytics(
                name="reacted",
                type=ActivityType.INTERACTION,
                member_activities_used=False,
                direction=ActivityDirection.EMITTER,
            ),
        ]

        raw_analytics: list[RawAnalytics] = [
            RawAnalytics(
                name="replied_per_acc",
                type=ActivityType.INTERACTION,
                member_activities_used=True,
                direction=ActivityDirection.EMITTER,
            ),
            RawAnalytics(
                name="mentioner_per_acc",
                type=ActivityType.INTERACTION,
                member_activities_used=True,
                direction=ActivityDirection.EMITTER,
            ),
            RawAnalytics(
                name="reacted_per_acc",
                type=ActivityType.INTERACTION,
                member_activities_used=True,
                direction=ActivityDirection.EMITTER,
            ),
        ]

        super().__init__(platform, resource_identifier, hourly_analytics, raw_analytics)
