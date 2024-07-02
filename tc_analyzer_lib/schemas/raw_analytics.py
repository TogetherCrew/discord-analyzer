from . import ActivityDirection, ActivityType


class RawAnalytics:
    def __init__(
        self,
        name: str,
        type: ActivityType | str,
        member_activities_used: bool,
        direction: ActivityDirection,
        activity_name: str | None = None,
        rawmemberactivities_condition: dict | None = None,
    ):
        self.name = name
        self.type = type if isinstance(type, ActivityType) else ActivityType(type)
        self.member_activities_used = member_activities_used
        self.direction = direction
        self.activity_name = activity_name
        self.rawmemberactivities_condition = rawmemberactivities_condition

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.value,
            "member_activities_used": self.member_activities_used,
            "direction": self.direction.value,
            "rawmemberactivities_condition": self.rawmemberactivities_condition,
            "activity_name": self.activity_name,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            name=data["name"],
            type=ActivityType(data["type"]),
            member_activities_used=data["member_activities_used"],
            direction=ActivityDirection(data["direction"]),
            activity_name=data.get("activity_name"),
            rawmemberactivities_condition=data.get("rawmemberactivities_condition"),
        )
