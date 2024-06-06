from . import ActivityType


class RawAnalytics:
    def __init__(self, name: str, type: ActivityType | str, member_activities_used: bool):
        self.name = name
        self.type = type if isinstance(type, ActivityType) else ActivityType(type)
        self.member_activities_used = member_activities_used

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.value,
            "member_activities_used": self.member_activities_used,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            name=data["name"],
            type=ActivityType(data["type"]),
            member_activities_used=data["member_activities_used"],
        )
