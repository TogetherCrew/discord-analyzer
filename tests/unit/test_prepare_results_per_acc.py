from tc_analyzer_lib.algorithms.utils.compute_interaction_mtx_utils import (
    prepare_per_account,
)


def test_empty_db_results():
    db_results_sample = []

    results = prepare_per_account(db_results=db_results_sample)

    assert results == {}


def test_single_document_db_results():
    db_results_sample = [
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "replied_per_acc": [],
        }
    ]

    results = prepare_per_account(db_results=db_results_sample)

    assert list(results.keys()) == ["968122690118512720"]
    assert results["968122690118512720"] == db_results_sample


def test_multiple_document_single_acc_db_results():
    db_results_sample = [
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "replied_per_acc": [],
        },
    ]

    results = prepare_per_account(db_results=db_results_sample)

    assert list(results.keys()) == ["968122690118512720"]
    assert results["968122690118512720"] == db_results_sample


def test_single_document_multiple_acc_db_results():
    db_results_sample = [
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512721",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 1}]],
            "replied_per_acc": [],
        },
    ]

    results = prepare_per_account(db_results=db_results_sample)

    assert list(results.keys()) == ["968122690118512720", "968122690118512721"]
    assert results["968122690118512720"] == [db_results_sample[0]]
    assert results["968122690118512721"] == [db_results_sample[1]]


def test_multiple_document_multiple_acc_db_results():
    db_results_sample = [
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 9}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 2}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512720",
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512721",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 3}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 4}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512721",
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [[{"account": "7952958225341480444", "count": 8}]],
        },
        {
            "user": "968122690118512720",
            "reacted_per_acc": [],
            "mentioner_per_acc": [
                [{"account": "7952958225341480444", "count": 5}],
                [{"account": "7952958225341480433", "count": 2}],
            ],
            "replied_per_acc": [],
        },
    ]

    results = prepare_per_account(db_results=db_results_sample)

    assert list(results.keys()) == ["968122690118512720", "968122690118512721"]
    assert results["968122690118512720"] == [
        {
            "user": "968122690118512720",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 9}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 2}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512720",
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512720",
            "reacted_per_acc": [],
            "mentioner_per_acc": [
                [{"account": "7952958225341480444", "count": 5}],
                [{"account": "7952958225341480433", "count": 2}],
            ],
            "replied_per_acc": [],
        },
    ]
    assert results["968122690118512721"] == [
        {
            "user": "968122690118512721",
            "reacted_per_acc": [[{"account": "795295822534148096", "count": 3}]],
            "mentioner_per_acc": [[{"account": "795295822534148096", "count": 4}]],
            "replied_per_acc": [],
        },
        {
            "user": "968122690118512721",
            "reacted_per_acc": [],
            "mentioner_per_acc": [],
            "replied_per_acc": [[{"account": "7952958225341480444", "count": 8}]],
        },
    ]
