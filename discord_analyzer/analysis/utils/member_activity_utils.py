from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pymongo
from discord_analyzer.analysis.compute_interaction_matrix_discord import (
    compute_interaction_matrix_discord,
)
from discord_analyzer.DB_operations.mongodb_access import DB_access
from networkx import DiGraph
from tc_core_analyzer_lib.assess_engagement import EngagementAssessment
from tc_core_analyzer_lib.utils.activity import DiscordActivity


def get_joined_accounts(db_access: DB_access, date_range: tuple[datetime, datetime]):
    """
    get the joined accounts for a time interval to a date range

    Parameters:
    -------------
    db_access: DB_access
        the database access class that queries are called through it
    date_range : tuple of datetime
        a tuple with length 2
        in the first index we save the starting date
        in the second date we would save the end date

    Returns:
    ----------
    data : list of dictionaries
        an array of dictionaries, each dictionary has `discordId` and `joined_at` member
    """
    query = {"joinedAt": {"$gte": date_range[0], "$lte": date_range[1]}}
    feature_projection = {"joinedAt": 1, "discordId": 1, "_id": 0}

    # quering the db now
    cursor = db_access.query_db_find("guildmembers", query, feature_projection)

    data = list(cursor)

    return data


def store_based_date(
    start_date,
    all_activities,
    analytics_day_range,
    joined_acc_dict,
    load_past,
    **kwargs,
):
    """
    store the activities (`all_*`) in a dictionary based on their ending analytics date

    Parameters:
    -------------
    start_date : datetime
        datetime object showing the start date of analysis
    all_activities : dictionary
        the `all_*` activities dictionary
        each key does have an activity, `all_joined_day`, `all_consistent`, etc
        and values are representing the analytics after the start_date
    analytics_day_range : int
        the range window of analytics
        to make sure that the dates of analytics is for the past
        `analytics_day_range` days, not `analytics_day_range` forward
    joined_acc_dict : array of dictionary
        an array of dictionaries, each dictionary has `discordId` and `joined_at` member
    load_past : bool
        whether we loaded the past data or start processing from scratch
        If True, indicates that the past data is loaded beside the analytics data
    **kwargs :
        empty_channel_acc : bool
            whether the channel and acc are empty
            if True, then this wouldn't give outputs
    """
    # to fill the all_joined_day field
    if "empty_channel_acc" in kwargs:
        if not kwargs["empty_channel_acc"]:
            return []

    # post processing the
    account_names = list(map(lambda record: record["discordId"], joined_acc_dict))
    acc_join_date = list(
        map(
            lambda record: record["joinedAt"].date(),
            joined_acc_dict,
        )
    )
    # converting to numpy to be easier to use
    account_names = np.array(account_names)
    acc_join_date = np.array(acc_join_date)

    # the data converted to multiple db records
    all_data_records = []

    # using the 3rd activity (2)
    # we do know it is always complete and have all the keys
    # finding the maximum days after first day of analytics
    max_days_after = len(all_activities[list(all_activities.keys())[2]])

    for day_index in range(max_days_after):
        analytics_date = start_date + timedelta(days=day_index)
        analytics_end_date = analytics_date + timedelta(days=analytics_day_range)
        # saving the data of a record
        data_record = {}

        if not load_past:
            date_using = analytics_end_date
        else:
            date_using = analytics_date

        data_record["date"] = date_using.isoformat()

        # analytics that were done in that date
        for activity in all_activities.keys():
            # if an analytics for that day was available
            if str(day_index) in all_activities[activity].keys():
                data_record[activity] = list(all_activities[activity][str(day_index)])
            # if there was no analytics in that day
            else:
                data_record[activity] = []

        # fill in the all_joined_day member
        data_record["all_joined_day"] = list(
            account_names[date_using.date() == acc_join_date]
        )

        # all_data_records[str(day_index)] = data_record
        all_data_records.append(data_record)

    # if there was no data just save empty date records
    if max_days_after == 0:
        data_record = {}
        data_record["date"] = (
            start_date + timedelta(days=analytics_day_range)
        ).isoformat()

        for activity in all_activities.keys():
            data_record[activity] = []

        all_data_records = [data_record]

    return all_data_records


def update_activities(past_activities, activities_list):
    """
    update activities variables using `past_activities` variable
    note: `past_activities` variable contains all the activities from past
    """
    from operator import itemgetter

    # getting all dictionary values with the order of `activities_list`
    activity_dictionaries = itemgetter(*activities_list)(past_activities)

    return activity_dictionaries


def convert_to_dict(data: list[Any], dict_keys: list[str]) -> dict[str, dict]:
    """
    convert data into dictionary
    Note: the length of data and dict_keys always must be the same

    Parameters:
    ------------
    data : list
        the data to use as dictionary values
    dict_keys : list
        the dictionary keys

    Returns:
    ---------
    converted_data : dict[str, dict]
        the data that is converted to dictionary
        with their corresponding keys
    """
    converted_data = dict(zip(dict_keys, data))

    return converted_data


def get_users_past_window(
    window_start_date: str,
    window_end_date: str,
    collection: pymongo.collection.Collection,
) -> list[str]:
    """
    get all users in the past date window from specific collection

    Parameters:
    ------------
    window_start_date : str
        the starting point of the window
        must be in format of the database which for now is %Y-%m-%d
    window_end_date : str
            the ending point of the window
            must be in format of the database which for now is %Y-%m-%d
    collection : pymongo.collection.Collection
        the mongodb collection to do the aggregation

    Returns:
    ---------
    user_names : list[str]
        the user names for the past 7 days
    """
    pipeline = [
        # Filter documents based on date
        {"$match": {"date": {"$gte": window_start_date, "$lte": window_end_date}}},
        {"$group": {"_id": "$account_name"}},
        {
            "$group": {
                "_id": None,
                "uniqueAccounts": {"$push": "$_id"},
            }
        },
    ]
    result = list(collection.aggregate(pipeline))

    # in case of no data we would return empty string
    user_names = []
    if result != []:
        user_names = result[0]["uniqueAccounts"]
        # removing remainder category
        if "remainder" in user_names:
            user_names.remove("remainder")

    return user_names


def get_latest_joined_users(db_access: DB_access, count: int = 5) -> list[str]:
    """
    get latest joined users

    Parameters:
    -------------
    db_access : DB_access
        database access class
    count : int
        the count of latest users to return

    Returns:
    ---------
    users : list[str]
        the userIds to use
    """
    cursor = db_access.query_db_find(
        table="guildmembers",
        query={"isBot": False},
        feature_projection={"discordId": 1, "_id": 0},
        sorting=("joinedAt", -1),
    ).limit(count)
    usersId = list(cursor)

    usersId = list(map(lambda x: x["discordId"], usersId))

    return usersId


def assess_engagement(
    w_i: int,
    accounts: list[str],
    action_params: dict[str, int],
    period_size: int,
    db_access: DB_access,
    channels: list[str],
    analyze_dates: list[str],
    activities_name: list[str],
    activity_dict: dict[str, dict],
    **kwargs,
) -> tuple[DiGraph, dict[str, dict]]:
    """
    assess engagement of a window index for users

    """
    activities_to_analyze = kwargs.get(
        "activities_to_analyze",
        [
            DiscordActivity.Mention,
            DiscordActivity.Reply,
            DiscordActivity.Reaction,
            DiscordActivity.Lone_msg,
            DiscordActivity.Thread_msg,
        ],
    )
    ignore_axis0 = kwargs.get(
        "ignore_axis0",
        [
            DiscordActivity.Mention,
        ],
    )
    # no need to ignore reactions
    ignore_axis1 = kwargs.get(
        "ignore_axis1",
        [
            DiscordActivity.Reply,
        ],
    )

    assess_engagment = EngagementAssessment(
        activities=activities_to_analyze,
        activities_ignore_0_axis=ignore_axis0,
        activities_ignore_1_axis=ignore_axis1,
    )
    # obtain interaction matrix
    int_mat = compute_interaction_matrix_discord(
        accounts,
        analyze_dates,
        channels,
        db_access,
        activities=activities_to_analyze,
    )

    # assess engagement
    (graph_out, *activity_dict) = assess_engagment.compute(
        int_mat=int_mat,
        w_i=w_i,
        acc_names=np.asarray(accounts),
        act_param=action_params,
        WINDOW_D=period_size,
        **activity_dict,
    )

    activity_dict = convert_to_dict(data=list(activity_dict), dict_keys=activities_name)
    return graph_out, activity_dict
