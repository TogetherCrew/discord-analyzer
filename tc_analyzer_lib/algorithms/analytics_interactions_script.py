import itertools


def per_account_interactions(
    cursor_list,
    dict_keys,
):
    """
    get per account interactions for each heatmaps fields

    Parameters:
    ------------
    cursor_list : list
        the db cursor returned and converted as list
    dict_keys : list
        the list of dictionary keys, representing the features in database

    Returns:
    ----------
    summed_per_account_interactions : dictionary
        the dictionary of each feature having summed the counts per hour,
          the dictionary of features is returned
    """
    data_processed = {}
    all_interaction_accounts = {}

    # for each interaction
    for k in dict_keys:
        temp_dict = {}
        # get the data of a key in a map
        samples = list(map(lambda data_dict: data_dict[k], cursor_list))

        # flatten the list
        samples_flattened = list(itertools.chain(*samples))

        for sample in samples_flattened:
            account_name = sample["account"]
            interaction_count = sample["count"]

            if account_name not in temp_dict.keys():
                temp_dict[account_name] = interaction_count
            else:
                temp_dict[account_name] += interaction_count

            if account_name not in all_interaction_accounts.keys():
                all_interaction_accounts[account_name] = interaction_count
            else:
                all_interaction_accounts[account_name] += interaction_count

        data_processed[k] = refine_dictionary(temp_dict)

    data_processed["all_interaction_accounts"] = refine_dictionary(
        all_interaction_accounts
    )

    summed_per_account_interactions = data_processed

    return summed_per_account_interactions


def refine_dictionary(interaction_dict):
    """
    refine dictionary and add the account id to the dictionary

    Parameters:
    ------------
    interaction_dict : dict
        a dictionary like {'user1': 5, 'user2: 4}
        keys are usernames and values are the count of each user interaction

    Returns:
    ----------
    refined_dict : nested dictionary
        the input refined like this
        {
            '0': { 'user1': 5 },
            '1': { 'user2': 4 }
        }
    """

    refined_dict = {}
    for idx, data_acc in enumerate(interaction_dict.keys()):
        refined_dict[f"{idx}"] = {
            "account": data_acc,
            "count": interaction_dict[data_acc],
        }

    return refined_dict
