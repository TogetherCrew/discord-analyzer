class RawAnalyticsItem:
    """
    Class for storing number of interactions per account
    """

    def __init__(self, account: str, count: int):
        self.account = account
        self.count = count

    def to_dict(self):
        return {"account": self.account, "count": self.count}
