from . import ActivityDirection, ActivityType


class HourlyAnalytics:
    def __init__(
        self,
        name: str,
        type: ActivityType,
        member_activities_used: bool,
        direction: ActivityDirection,
        rawmemberactivities_condition: dict | None = None,
        activity_name: str | None = None,
    ):
        self.name = name
        self.type = type
        self.direction = direction
        self.member_activities_used = member_activities_used
        self.activity_name = activity_name
        self.rawmemberactivities_condition = rawmemberactivities_condition

    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.type.value,
            "direction": self.direction.value,
            "member_activities_used": self.member_activities_used,
            "activity_name": self.activity_name,
        }
        if self.rawmemberactivities_condition:
            result["rawmemberactivities_condition"] = self.rawmemberactivities_condition

        return result

    @classmethod
    def from_dict(cls, data: dict):
        rawmemberactivities_condition = data.get("rawmemberactivities_condition")

        return cls(
            name=data["name"],
            type=ActivityType(data["type"]),
            member_activities_used=data["member_activities_used"],
            direction=ActivityDirection(data["direction"]),
            activity_name=data.get("activity_name"),
            rawmemberactivities_condition=rawmemberactivities_condition,
        )
