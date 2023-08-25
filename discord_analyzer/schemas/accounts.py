class AccountCounts:
    """
    Class for storing number of interactions per account
    """

    # define constructor
    def __init__(self, account, counts):
        self.account = account  # account name
        self.counts = counts  # number of interactions

    # convert as dict
    def asdict(self):
        return ({"account": self.account, "count": self.counts},)
