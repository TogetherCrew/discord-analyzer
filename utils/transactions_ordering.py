import numpy as np
from tc_messageBroker.rabbit_mq.status import Status


def sort_transactions(transactions: list):
    """
    sort transactions by their order and status
    the NOT_STARTED ones would be at the first of the list
    and they are ordered by `order` property

    Parameters:
    ------------
    transactions : list[ITransaction]
        the list of transactions to order

    Returns:
    ---------
    transactions_ordered : ndarray(ITransaction)
        the transactions ordered by status
        the `NOT_STARTED` ones are the firsts
        it is actually a numpy array for us to be able to
            change the properties in deep memory
    tx_not_started_count : int
        the not started transactions count
    """
    tx_not_started = []
    tx_other = []

    for tx in transactions:
        if tx.status == Status.NOT_STARTED:
            tx_not_started.append(tx)
        else:
            tx_other.append(tx)

    tx_not_started_count = len(tx_not_started)
    tx_not_started_sorted = sort_transactions_orderly(tx_not_started)

    transactions_ordered = list(tx_not_started_sorted)
    transactions_ordered.extend(tx_other)

    return np.array(transactions_ordered), tx_not_started_count


def sort_transactions_orderly(transactions: list):
    """
    sort transactions by their `order` property

    Parameters:
    ------------
    transactions : list[ITransaction]
        the list of transactions to order

    Returns:
    ---------
    transactions_orderly_sorted : list[ITransaction]
        transactions sorted by their order
    """
    orders = [tx.order for tx in transactions]
    sorted_indices = np.argsort(orders)

    return np.array(transactions)[sorted_indices]
