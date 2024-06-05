from . import ActivityType


class HourlyAnalytics:
    def __init__(
        self,
        name: str,
        type: ActivityType,
        member_activities_used: bool,
        metadata_condition: dict | None = None,
    ):
        self.name = name
        self.type = type
        self.member_activities_used = member_activities_used
        self.metadata_condition = metadata_condition

    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.type.value,
            "member_activities_used": self.member_activities_used,
        }
        if self.metadata_condition:
            result["metadata_condition"] = self.metadata_condition

        return result

    @classmethod
    def from_dict(cls, data: dict):
        metadata_condition = data.get("metadata_condition")

        return cls(
            name=data["name"],
            type=ActivityType(data["type"]),
            member_activities_used=data["member_activities_used"],
            metadata_condition=metadata_condition,
        )