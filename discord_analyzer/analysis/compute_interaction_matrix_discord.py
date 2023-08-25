#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  compute_interaction_matrix_discord.py
#
#  Author Ene SS Rawa / Tjitse van der Molen

from numpy import ndarray

from discord_analyzer.analysis.utils.activity import Activity
from discord_analyzer.DB_operations.mongodb_query import MongodbQuery

from .utils.compute_interaction_mtx_utils import (  # isort: skip
    generate_interaction_matrix,
    prepare_per_account,
)


def compute_interaction_matrix_discord(
    acc_names,
    dates,
    channels,
    db_access,
    activities: list[Activity] = [Activity.Mention, Activity.Reply, Activity.Reaction],
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
        default is to include all 3 `Activity` types
        minimum length is 1

    Output:
    ---------
    int_mtx : dict[str, np.ndarray]
        keys are representative of an activity
        and the 2d matrix representing the interactions for the activity
    """

    feature_projection = {
        "thr_messages": 0,
        "lone_messages": 0,
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
        dates=dates,
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

    ###### And now compute the interactions per account_name (`acc`) ######
    int_mat = {}
    # computing `int_mat` per activity
    for activity in activities:
        int_mat[activity] = generate_interaction_matrix(
            per_acc_interactions=per_acc_query_result,
            acc_names=acc_names,
            activities=[activity],
        )

    return int_mat
