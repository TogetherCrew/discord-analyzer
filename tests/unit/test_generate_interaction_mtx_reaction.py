from discord_analyzer.analysis.utils.activity import Activity
from discord_analyzer.analysis.utils.compute_interaction_mtx_utils import \
    generate_interaction_matrix


def test_single_account_reaction():
    per_acc_interactions = {
        "968122690118512720": [
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [[{"account": "968122690118512720", "count": 1}]],
                "mentioner_per_acc": [[{"account": "968122690118512720", "count": 2}]],
                "replied_per_acc": [],
            },
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [[{"account": "968122690118512720", "count": 7}]],
                "mentioner_per_acc": [[{"account": "968122690118512720", "count": 4}]],
                "replied_per_acc": [[{"account": "968122690118512720", "count": 3}]],
            },
        ]
    }
    int_mtx = generate_interaction_matrix(
        per_acc_interactions,
        acc_names=["968122690118512720"],
        activities=[Activity.Reaction],
    )

    # converting `numpy.bool_` to python `bool`
    is_match = bool((int_mtx == [[8]]).all())
    assert is_match is True


def test_two_accounts_reaction():
    acc_names = ["968122690118512720", "968122690118512799"]
    per_acc_interactions = {
        "968122690118512720": [
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [[{"account": "968122690118512799", "count": 1}]],
                "mentioner_per_acc": [[{"account": "968122690118512799", "count": 3}]],
                "replied_per_acc": [],
            },
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [[{"account": "968122690118512720", "count": 2}]],
                "mentioner_per_acc": [[{"account": "968122690118512720", "count": 1}]],
                "replied_per_acc": [],
            },
        ]
    }

    int_mtx = generate_interaction_matrix(
        per_acc_interactions, acc_names=acc_names, activities=[Activity.Reaction]
    )
    # converting `numpy.bool_` to python `bool`
    is_match = bool((int_mtx == [[2, 1], [0, 0]]).all())
    assert is_match is True


def test_multiple_interactions_reaction():
    acc_names = [
        "968122690118512720",
        "795295822534148096",
        "968122690118512721",
        "7952958225341480444",
        "7952958225341480433",
    ]
    per_acc_interactions = {
        "968122690118512720": [
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [[{"account": "795295822534148096", "count": 9}]],
                "mentioner_per_acc": [[{"account": "795295822534148096", "count": 2}]],
                "replied_per_acc": [],
            },
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [],
                "mentioner_per_acc": [],
                "replied_per_acc": [],
            },
            {
                "account_name": "968122690118512720",
                "reacted_per_acc": [
                    [{"account": "7952958225341480444", "count": 7}],
                    [{"account": "7952958225341480433", "count": 1}],
                ],
                "mentioner_per_acc": [
                    [{"account": "7952958225341480444", "count": 5}],
                    [{"account": "7952958225341480433", "count": 2}],
                ],
                "replied_per_acc": [],
            },
        ],
        "968122690118512721": [
            {
                "account_name": "968122690118512721",
                "reacted_per_acc": [[{"account": "795295822534148096", "count": 3}]],
                "mentioner_per_acc": [[{"account": "795295822534148096", "count": 4}]],
                "replied_per_acc": [],
            },
            {
                "account_name": "968122690118512721",
                "reacted_per_acc": [],
                "mentioner_per_acc": [],
                "replied_per_acc": [[{"account": "7952958225341480444", "count": 8}]],
            },
        ],
    }

    int_mtx = generate_interaction_matrix(
        per_acc_interactions, acc_names=acc_names, activities=[Activity.Reaction]
    )
    print(int_mtx)
    assert int_mtx.shape == (5, 5)
    is_match = (
        int_mtx
        == [
            [0.0, 9.0, 0.0, 7.0, 1.0],
            [0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 3.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0],
        ]
    ).all()
    assert bool(is_match) is True
