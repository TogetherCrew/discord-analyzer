class AutomationTrigger:
    def __init__(self, options: dict, enabled: bool):
        # self.options = {"category": category}
        self.options = options
        self.enabled = enabled


class AutomationAction:
    def __init__(self, template: str, options: dict, enabled: bool):
        self.template = template
        self.options = options
        self.enabled = enabled


class AutomationReport:
    def __init__(
        self, recipientIds: list[str], template: str, options: dict, enabled: bool
    ):
        self.recipientIds = recipientIds
        self.template = template
        self.options = options
        self.enabled = enabled


class Automation:
    def __init__(
        self,
        guild_id: str,
        triggers: list[AutomationTrigger],
        actions: list[AutomationAction],
        report: AutomationReport,
        enabled: bool,
    ):
        self.guild_id = guild_id
        self.triggers = triggers
        self.actions = actions
        self.report = report
        self.enabled = enabled

    @classmethod
    def from_dict(cls, data: dict):
        triggers = [AutomationTrigger(**trigger) for trigger in data["triggers"]]
        actions = [AutomationAction(**trigger) for trigger in data["actions"]]
        report = AutomationReport(**data["report"])
        return cls(data["guildId"], triggers, actions, report, data["enabled"])

    def to_dict(self):
        return {
            "guildId": self.guild_id,
            "triggers": [trigger.__dict__ for trigger in self.triggers],
            "actions": [action.__dict__ for action in self.actions],
            "report": self.report.__dict__,
            "enabled": self.enabled,
        }
