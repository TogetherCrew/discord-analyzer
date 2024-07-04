from enum import Enum


class ActivityType(Enum):
    ACTION = "actions"
    INTERACTION = "interactions"


class ActivityDirection(Enum):
    RECEIVER = "receiver"
    EMITTER = "emitter"
