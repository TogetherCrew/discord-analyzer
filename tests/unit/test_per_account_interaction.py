from tc_analyzer_lib.algorithms.analytics_interactions_script import (
    per_account_interactions,
)


def test_per_account_interaction_no_inputs():
    sample_input = []

    results = per_account_interactions(
        sample_input,
        dict_keys=[
            "mentioner_accounts",
            "reacter_accounts",
            "replier_accounts",
        ],
    )

    assert results["mentioner_accounts"] == {}
    assert results["reacter_accounts"] == {}
    assert results["replier_accounts"] == {}
    assert results["all_interaction_accounts"] == {}


def test_per_account_interaction_empty_inputs():
    sample_input = [
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [],
            "reacter_accounts": [],
            "replier_accounts": [],
        },
        {
            "user": "acc2",
            "channel_id": "321",
            "mentioner_accounts": [],
            "reacter_accounts": [],
            "replier_accounts": [],
        },
        {
            "user": "acc2",
            "channel_id": "555",
            "mentioner_accounts": [],
            "reacter_accounts": [],
            "replier_accounts": [],
        },
    ]

    results = per_account_interactions(
        sample_input,
        dict_keys=[
            "mentioner_accounts",
            "reacter_accounts",
            "replier_accounts",
        ],
    )

    assert results["mentioner_accounts"] == {}
    assert results["reacter_accounts"] == {}
    assert results["replier_accounts"] == {}
    assert results["all_interaction_accounts"] == {}


def test_per_account_interaction_accounts():
    sample_input = [
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 1}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 1}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 1}],
            "reacter_accounts": [{"account": "Mehrdad", "count": 1}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 10}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 2}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "546",
            "mentioner_accounts": [{"account": "mramin22#1669", "count": 10}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 2}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "000",
            "mentioner_accounts": [{"account": "mramin22#1669", "count": 10}],
            "reacter_accounts": [{"account": "Behzad", "count": 6}],
            "replier_accounts": [{"account": "Behzad", "count": 7}],
        },
    ]

    # the accounts used above
    mentioner_accounts_names = ["mramin22#1669", "Ene SS Rawa#0855"]
    reacter_accounts_names = ["ahmadyazdanii#7517", "Mehrdad", "Behzad"]
    replier_accounts_names = ["Behzad", "ahmadyazdanii#7517"]

    results = per_account_interactions(
        sample_input,
        dict_keys=[
            "mentioner_accounts",
            "reacter_accounts",
            "replier_accounts",
        ],
    )

    # the whole results assersion
    assert set(results.keys()) == set(
        [
            "replier_accounts",
            "reacter_accounts",
            "mentioner_accounts",
            "all_interaction_accounts",
        ]
    )

    # mentioner_accounts assersions
    action_type = "mentioner_accounts"
    assert len(results[action_type].values()) == len(mentioner_accounts_names)
    assert results[action_type]["0"]["account"] in mentioner_accounts_names
    assert results[action_type]["1"]["account"] in mentioner_accounts_names

    # reacter_accounts assersions
    action_type = "reacter_accounts"
    assert len(results[action_type].values()) == len(reacter_accounts_names)
    assert results[action_type]["0"]["account"] in reacter_accounts_names
    assert results[action_type]["1"]["account"] in reacter_accounts_names
    assert results[action_type]["2"]["account"] in reacter_accounts_names

    # replier_accounts assersions
    action_type = "replier_accounts"
    assert len(results[action_type].values()) == len(replier_accounts_names)
    assert results[action_type]["0"]["account"] in replier_accounts_names
    assert results[action_type]["1"]["account"] in replier_accounts_names


def test_per_account_interaction_numbers():
    sample_input = [
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 1}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 1}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 1}],
            "reacter_accounts": [{"account": "Mehrdad", "count": 1}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "1234",
            "mentioner_accounts": [{"account": "Ene SS Rawa#0855", "count": 10}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 2}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "546",
            "mentioner_accounts": [{"account": "mramin22#1669", "count": 10}],
            "reacter_accounts": [{"account": "ahmadyazdanii#7517", "count": 2}],
            "replier_accounts": [{"account": "ahmadyazdanii#7517", "count": 5}],
        },
        {
            "user": "acc1",
            "channel_id": "000",
            "mentioner_accounts": [{"account": "mramin22#1669", "count": 10}],
            "reacter_accounts": [{"account": "Behzad", "count": 6}],
            "replier_accounts": [{"account": "Behzad", "count": 7}],
        },
    ]

    account_sum_interaction = {
        "Ene SS Rawa#0855": 12,
        "ahmadyazdanii#7517": 25,
        "Mehrdad": 1,
        "mramin22#1669": 20,
        "Behzad": 13,
    }

    results = per_account_interactions(
        sample_input,
        dict_keys=[
            "mentioner_accounts",
            "reacter_accounts",
            "replier_accounts",
        ],
    )

    # 5 users we had
    assert len(results["all_interaction_accounts"].values()) == 5

    # check each user interaction
    for i in range(5):
        account_res = list(results["all_interaction_accounts"].values())

        acc_name = account_res[i]["account"]
        acc_interaction_count = account_res[i]["count"]
        assert acc_name in account_sum_interaction.keys()
        assert account_sum_interaction[acc_name] == acc_interaction_count
