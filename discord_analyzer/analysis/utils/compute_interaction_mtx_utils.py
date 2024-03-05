import logging
from typing import Any

import numpy as np
from discord_analyzer.analysis.analytics_interactions_script import (
    per_account_interactions,
)
from tc_core_analyzer_lib.utils.activity import DiscordActivity


def prepare_per_account(db_results: list) -> dict[str, list[dict]]:
    """
    convert the db_results into per account results

    Parameters:
    ------------
    db_results : list[Any]
        the results gotten from heatmaps

    Returns:
    ---------
    per_acc_query_result : dict[str, list[dict]]
        per account results
        key is the account name
        and values are the docuemnts of database
    """
    # Cetegorize per account_name
    per_acc_query_result: dict[str, list[dict]] = {}

    # a dictionary for results of each account
    for db_record in db_results:
        acc_name = db_record["account_name"]
        per_acc_query_result.setdefault(acc_name, [])
        per_acc_query_result[acc_name].append(db_record)

    return per_acc_query_result


def generate_interaction_matrix(
    per_acc_interactions: dict[str, list[Any]],
    acc_names: list[str],
    activities: list[str],
) -> np.ndarray:
    """
    generate interaction matrix for account interactions

    Parameters:
    ------------
    per_acc_interactions : dict[str, list[Any]]
        dictionary of per account interactions
        keys are the account names
        values are the interactions for that account
    acc_names : [str]
        list of all account names to be considered for analysis
    activities : list[str]
        the activities to include for generating interaction matrix
        min length is 1

    Returns:
    ---------
    int_matrix : np.ndarray
        an array of integer values
        each row and column are representative of account interactions
    """
    int_matrix = np.zeros((len(acc_names), len(acc_names)), dtype=np.uint16)

    for acc in per_acc_interactions.keys():
        db_res_per_acc = per_acc_interactions[acc]

        dict_keys = prepare_interaction_field_names(activities=activities)
        # get results from db
        db_results = per_account_interactions(
            cursor_list=db_res_per_acc,
            dict_keys=dict_keys,
        )

        # obtain results for all interactions summed together
        acc_out_int = db_results["all_interaction_accounts"]

        # for each interacting account
        for int_acc in acc_out_int.values():
            # if the interacting account is in acc_names
            if int_acc["account"] in acc_names:
                # store data in int_network
                int_matrix[
                    np.where(np.array(acc_names) == acc)[0][0],
                    np.where(np.array(acc_names) == int_acc["account"])[0][0],
                ] = int_acc["count"]

    return int_matrix


def prepare_interaction_field_names(activities: list[str]) -> list[str]:
    """
    convert activity names to the field names
    as are saved under the heatmaps collection


    Parameters:
    ------------
    activities : list[str]
        the activities to be converted to db field names
        could be the items below
        - `mention`
        - `reply`
        - `reaction`

    Returns:
    ---------
    field_names : list[str]
        the field names from database to use
    """
    field_names = []
    for activity in activities:
        if activity == DiscordActivity.Mention:
            field_names.append("mentioner_per_acc")
        elif activity == DiscordActivity.Reply:
            field_names.append("replied_per_acc")
        elif activity == DiscordActivity.Reaction:
            field_names.append("reacted_per_acc")
        elif activity == DiscordActivity.Thread_msg:
            field_names.append("thr_messages")
        elif activity == DiscordActivity.Lone_msg:
            field_names.append("lone_messages")
        else:
            logging.warning("prepare_interaction_field_names: Wrong activity given!")

    return field_names
