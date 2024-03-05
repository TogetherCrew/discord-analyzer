#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  compute_interaction_matrix_discord.py
#
#  Author Ene SS Rawa / Tjitse van der Molen
from typing import Any
import copy
from discord_analyzer.DB_operations.mongodb_access import DB_access
from discord_analyzer.DB_operations.mongodb_query import MongodbQuery
from tc_core_analyzer_lib.utils.activity import DiscordActivity
from numpy import ndarray, diag_indices_from

from .utils.compute_interaction_mtx_utils import (
    generate_interaction_matrix,
    prepare_per_account,
)


def compute_interaction_matrix_discord(
    acc_names: list[str],
    dates: list[str],
    channels: list[str],
    db_access: DB_access,
    activities: list[str] = [
        DiscordActivity.Mention,
        DiscordActivity.Reply,
        DiscordActivity.Reaction,
        DiscordActivity.Lone_msg,
        DiscordActivity.Thread_msg,
    ],
) -> dict[str, ndarray]:
    """
    Computes interaction matrix from discord data

    Input:
    --------
    acc_names - [str] : list of all account names to be considered for analysis
    dates - [str] : list of all dates to be considered for analysis
    channels - [str] : list of all channel ids to be considered for analysis
    db_access - obj : database access object
    activities - list[Activity] :
        the list of activities to generate the matrix for
        default is to include all activity types
        minimum length is 1

    Output:
    ---------
    int_mtx : dict[str, np.ndarray]
        keys are representative of an activity
        and the 2d matrix representing the interactions for the activity
    """

    feature_projection = {
        "channelId": 0,
        "replier": 0,
        "replied": 0,
        "mentioner": 0,
        "mentioned": 0,
        "reacter": 0,
        "reacted": 0,
        "__v": 0,
        "_id": 0,
    }

    # intiate query
    query = MongodbQuery()

    # set up query dictionary
    query_dict = query.create_query_filter_account_channel_dates(
        acc_names=acc_names,
        channels=channels,
        dates=list(dates),
        date_key="date",
        channel_key="channelId",
        account_key="account_name",
    )

    # create cursor for db
    cursor = db_access.query_db_find(
        table="heatmaps", query=query_dict, feature_projection=feature_projection
    )
    db_results = list(cursor)

    per_acc_query_result = prepare_per_account(db_results=db_results)
    per_acc_interaction = process_non_reactions(per_acc_query_result)

    # And now compute the interactions per account_name (`acc`)
    int_mat = {}
    # computing `int_mat` per activity
    for activity in activities:
        int_mat[activity] = generate_interaction_matrix(
            per_acc_interactions=per_acc_interaction,
            acc_names=acc_names,
            activities=[activity],
        )
        # a person interacting to themselves is not counted as activity
        if activity in [
            DiscordActivity.Reply,
            DiscordActivity.Reaction,
            DiscordActivity.Mention,
        ]:
            int_mat[activity][diag_indices_from(int_mat[activity])] = 0

    return int_mat


def process_non_reactions(
    heatmaps_data_per_acc: dict[str, list[dict[str, Any]]],
    skip_fields: list[str] = [
        "reacted_per_acc",
        "mentioner_per_acc",
        "replied_per_acc",
        "account_name",
        "date",
    ],
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
                        [{"account": account, "count": sum(document[action])}]
                    ]
                else:
                    # action count was zero
                    document[action] = []

    return heatmaps_interactions_per_acc
