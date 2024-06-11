from enum import Enum


class ActivityType(Enum):
    ACTION = "action"
    INTERACTION = "interaction"

class ActivityDirection(Enum):
    RECEIVER = "receiver"
    EMITTER = "emitter"
