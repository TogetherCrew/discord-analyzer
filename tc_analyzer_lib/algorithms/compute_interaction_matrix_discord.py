import copy
from datetime import datetime
from typing import Any

from numpy import diag_indices_from, ndarray
from tc_analyzer_lib.utils.mongo import MongoSingleton

from .utils.compute_interaction_mtx_utils import (
    generate_interaction_matrix,
    prepare_per_account,
)


def compute_interaction_matrix_discord(
    acc_names: list[str],
    date_range: tuple[datetime, datetime],
    resources: list[str],
    resource_identifier: str,
    platform_id: str,
    interactions: list[str],
    actions: list[str],
) -> dict[str, ndarray]:
    """
    Computes interaction matrix from discord data

    Parameters:
    -------------
    acc_names : list[str]
        list of all account names to be considered for analysis
    date_range : list[datetime, datetime]
        a list with length 2
        the first index is starting date range
        the seocnd index is ending date range
    resources : list[str]
        list of all resource id to be considered for analysis
    resource_identifier : str
        the identifier for resource ids
        could be `channel_id` for discord
    platform_id : str
        the platform to fetch its data from
    interactions : list[str]
        the list of interaction activities to generate the matrix for
        minimum length is 1
    actions : list[str]
        the list of action activities to generate the matrix for
        we would assume actions as self-interactions in matrix
        minimum length is 1

    Output:
    ---------
    int_mtx : dict[str, np.ndarray]
        keys are representative of an activity
        and the 2d matrix representing the interactions for the activity
    """
    client = MongoSingleton.get_instance().get_client()
    feature_projection: dict[str, bool] = {
        activity: True for activity in actions + interactions
    }

    feature_projection = {
        **feature_projection,
        "user": True,
    }
    query = {
        "$and": [
            {"user": {"$in": acc_names}},
            {resource_identifier: {"$in": resources}},
            {
                "date": {
                    "$gte": date_range[0],
                    "$lt": date_range[1],
                }
            },
        ]
    }

    cursor = client[platform_id]["heatmaps"].find(
        query,
        feature_projection,
    )
    db_results = list(cursor)

    per_acc_query_result = prepare_per_account(db_results=db_results)
    per_acc_interaction = process_actions(
        per_acc_query_result, skip_fields=[*interactions, "user", "_id"]
    )

    # And now compute the interactions per account_name (`acc`)
    int_mat = {}
    # computing `int_mat` per activity
    for activity in interactions + actions:
        int_mat[activity] = generate_interaction_matrix(
            per_acc_interactions=per_acc_interaction,
            acc_names=acc_names,
            activities=[activity],
        )
        # removing self-interactions
        if activity in interactions:
            int_mat[activity][diag_indices_from(int_mat[activity])] = 0

    return int_mat


def process_actions(
    heatmaps_data_per_acc: dict[str, list[dict[str, Any]]],
    skip_fields: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """
    process the non-interactions heatmap data to be like interaction
    we will make it self interactions

    Parameters
    -----------
    heatmaps_data_per_acc : dict[str, list[dict[str, Any]]]
        heatmaps data per account
        the keys are accounts
        and the values are the list of heatmaps documents related to them
    skip_fields : list[str]
        the part of heatmaps document that we don't need to make them like interaction
        can be interactions itself and account_name, and date

    Returns
    --------
    heatmaps_interactions_per_acc : dict[str, list[dict[str, Any]]]
        the same as before but we have changed the non interaction ones to self interaction
    """
    heatmaps_interactions_per_acc = copy.deepcopy(heatmaps_data_per_acc)

    for account in heatmaps_interactions_per_acc.keys():
        # for each heatmaps document
        for document in heatmaps_interactions_per_acc[account]:
            activities = document.keys()
            actions = set(activities) - set(skip_fields)

            for action in actions:
                action_count = sum(document[action])
                if action_count:
                    document[action] = [
                        {"account": account, "count": sum(document[action])}
                    ]
                else:
                    # action count was zero
                    document[action] = []

    return heatmaps_interactions_per_acc
