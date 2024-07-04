#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  member_activity_history.py
#
#  Author Ene SS Rawa / Tjitse van der Molen

import logging
from datetime import datetime, timedelta

import networkx as nx
import numpy as np
from dateutil.relativedelta import relativedelta
from tc_analyzer_lib.algorithms.member_activity_history import check_past_history
from tc_analyzer_lib.algorithms.utils.member_activity_history_utils import (
    MemberActivityPastUtils,
)
from tc_analyzer_lib.algorithms.utils.member_activity_utils import (
    assess_engagement,
    convert_to_dict,
    get_joined_accounts,
    get_latest_joined_users,
    get_users_past_window,
    store_based_date,
    update_activities,
)
from tc_analyzer_lib.DB_operations.mongodb_access import DB_access
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase


def compute_member_activity(
    platform_id: str,
    resources: list[str],
    resource_identifier: str,
    acc_names: list[str],
    date_range: list[datetime],
    analyzer_config: PlatformConfigBase,
    window_param: dict[str, int],
    act_param: dict[str, int],
    load_past_data=True,
):
    """
    Computes member activity and member interaction network

    Parameters
    ------------
    platform_id: (str) - platform id
    resources: [str] - list of all resource ids that should be analysed
    acc_names: [str] - list of all account names that should be analysed
    date_range: tuple[datetime, datetime] - tuple of first and last date to be analysed (one output per date)
    analyzer_config : PlatformConfigBase
        the config for the analyzer to use.
        representing which analytics to compute
    window_param: dict[str, int] -
        "period_size": window size in days. default = 7
        "step_size": step size of sliding window in days. default = 1
        (Currently these values will be default values,
        in the future, the user might be able to set these in the
        extraction settings page)
    act_param : dict[str, int]
        parameters for activity types:
        keys are listed below
            - INT_THR : int
                minimum number of interactions to be active
            - UW_DEG_THR : int
                minimum number of connections to be active
            - EDGE_STR_THR : int
                minimum number of interactions for connected
            - UW_THR_DEG_THR : int
                minimum number of accounts for connected
            - CON_T_THR : int
                time period to assess consistently active
            - CON_O_THR : int
                times to be active within CON_T_THR to be
            consistently active
            - VITAL_T_THR : int
                time period to assess for vital
            - VITAL_O_THR : int
                times to be connected within VITAL_T_THR to be vital
            - PAUSED_T_THR : int
                time period to remain paused
            - STILL_T_THR : int
                time period to assess for still active
            - STILL_O_THR : int
                times to be active within STILL_T_THR to be still active
            - DROP_H_THR : int
                time periods in the past to have been newly active
            - DROP_I_THR : int
                time periods to have been inactive
        (Currently these values will be default values,
          in the future, the user might be able to adjust these)

    Returns
    ----------
    network_dict: {datetime:networkx obj} -
        dictionary with python datetime objects as keys and networkx graph
        objects as values.
        The keys reflect the last date of the WINDOW_D day window
        over which the network was computed.
        The values contain the computed networks.
    activity_dict: {str:{str:set}} -
        dictionary with keys reflecting each member activity type and
        dictionaries as values. Each nested dictionary contains an index string as
        key reflecting the number of STEP_D steps have been
        taken since the first analysis period. The values in the nested dictionary
        are python sets with account names that belonged to that category
        in that period. The length of the set reflects the total number.
    load_past_data : bool
        whether to load past data or not, default is True
        if True, will load the past data, if data was available in given range
    """
    platform_msg = f"PLATFORM_ID: {platform_id}:"

    # set up database access
    db_access = DB_access(platform_id)

    # specify the features not to be returned

    # initiate result dictionary for network graphs
    network_dict = {}

    # initiate result dictionaries for engagement types
    activity_dict: dict[str, dict] = {
        "all_joined": {},
        "all_joined_day": {},
        "all_consistent": {},
        "all_vital": {},
        "all_active": {},
        "all_connected": {},
        "all_paused": {},
        "all_new_disengaged": {},
        "all_disengaged": {},
        "all_unpaused": {},
        "all_returned": {},
        "all_new_active": {},
        "all_still_active": {},
        "all_dropped": {},
        "all_disengaged_were_newly_active": {},
        "all_disengaged_were_consistently_active": {},
        "all_disengaged_were_vital": {},
        "all_lurker": {},
        "all_about_to_disengage": {},
        "all_disengaged_in_past": {},
    }
    activities_name = list(activity_dict.keys())

    if load_past_data:
        # past_activities_date is the data from past activities
        # new_date_range is defined to change the date_range with past data loaded
        # starting_key is the starting key of actuall analysis
        past_activities_data, new_date_range, starting_key = check_past_history(
            db_access=db_access,
            date_range=date_range,
            collection_name="memberactivities",
            window_param=window_param,
        )
    else:
        past_activities_data = {}
        new_date_range = [date_range[0], date_range[1]]
        starting_key = 0

    # if in past there was an activity, we'll update the dictionaries
    if past_activities_data != {}:
        activities = update_activities(
            past_activities=past_activities_data, activities_list=activities_name
        )
        activity_dict = convert_to_dict(
            data=list(activities), dict_keys=activities_name
        )

    # if there was still a need to analyze some data in the range
    # also if there was some accounts and channels to be analyzed
    if new_date_range != []:
        # all_joined data

        # if the date range wasn't as long as a date window,
        # no analytics for the days would be computed
        # so make it as a window_d lenght to have the computations
        new_date_range_interval = (new_date_range[1] - new_date_range[0]).days
        if load_past_data is True:
            interval_before = (new_date_range_interval) + (
                window_param["period_size"] - 1
            )
            new_date_range[0] = new_date_range[1] - timedelta(days=interval_before)

        member_activity_utils = MemberActivityPastUtils(db_access=db_access)
        (
            activity_dict["all_joined"],
            activity_dict["all_joined_day"],
        ) = member_activity_utils.update_joined_accounts(
            start_dt=new_date_range[0],
            end_dt=new_date_range[1],
            all_joined_day=activity_dict["all_joined_day"],
            starting_key=starting_key,
            window_d=window_param["period_size"],
        )

        # # # DEFINE SLIDING WINDOW RANGE # # #

        # determine window start times
        start_dt = new_date_range[0]
        end_dt = new_date_range[1]

        time_diff = end_dt - start_dt

        # determine maximum start time (include last day in date_range)
        last_start = time_diff - relativedelta(days=window_param["period_size"] - 1)

        # # # ACTUAL ANALYSIS # # #

        # for every window index
        max_range = int(np.floor(last_start.days / window_param["step_size"]) + 1)
        # if max range was chosen negative,
        # then we have to make it zero
        # (won't affect the loop but will affect codes after it)
        if max_range < 0:
            max_range = 0
        if acc_names != [] and resources != []:
            for w_i in range(max_range):
                msg_info = "MEMBERACTIVITY ANALYTICS: PROGRESS"
                msg = f"{platform_msg} {msg_info} {w_i + 1}/{max_range}"
                logging.info(msg)
                new_window_i = w_i + starting_key

                last_date = (
                    new_date_range[0]
                    + relativedelta(days=window_param["step_size"] * w_i)
                    + relativedelta(days=window_param["period_size"])
                )

                window_start = last_date - relativedelta(
                    days=window_param["period_size"]
                )

                # updating account names for past 7 days
                acc_names = get_users_past_window(
                    window_start_date=window_start,
                    window_end_date=last_date,
                    collection=db_access.db_mongo_client[platform_id]["heatmaps"],
                )

                if acc_names == []:
                    time_window_str = f"{window_start.strftime('%Y-%m-%d')} - "
                    time_window_str += last_date.strftime("%Y-%m-%d")
                    logging.warning(
                        f"{platform_msg} No data for the time window {time_window_str}"
                    )
                    logging.info(
                        "Getting latest joined instead! "
                        "So we could compute other activity types!"
                    )

                    # will get 5 users just to make sure
                    # we could have empty outputs
                    acc_names = get_latest_joined_users(db_access, count=5)

                graph_out, activity_dict = assess_engagement(
                    w_i=new_window_i,
                    accounts=acc_names,
                    action_params=act_param,
                    period_size=window_param["period_size"],
                    platform_id=platform_id,
                    resources=resources,
                    resource_identifier=resource_identifier,
                    analyze_dates=(
                        last_date - timedelta(days=window_param["period_size"]),
                        last_date,
                    ),
                    activities_name=activities_name,
                    activity_dict=activity_dict,
                    analyzer_config=analyzer_config,
                )

                # make empty dict for node attributes
                node_att = {}

                # store account names in node_att dict
                for i, node in enumerate(list(graph_out)):
                    node_att[node] = acc_names[i]

                # assign account names in node_att to node attributes of graph_out
                nx.set_node_attributes(graph_out, node_att, "acc_name")

                # store results in dictionary
                network_dict[last_date - timedelta(days=1)] = graph_out
    # else if there was no past data
    else:
        max_range = 0

    start_dt = date_range[0]
    end_dt = date_range[1]

    # get the accounts with their joining date
    joined_acc_dict = get_joined_accounts(
        db_access=db_access, date_range=(start_dt, end_dt + timedelta(days=1))
    )

    activity_dict_per_date = store_based_date(
        start_date=start_dt,
        all_activities=activity_dict,
        analytics_day_range=window_param["period_size"] - 1,
        joined_acc_dict=joined_acc_dict,
        load_past=load_past_data,
        empty_channel_acc=(len(resources) != 0 and len(acc_names) != 0),
    )

    return [network_dict, activity_dict_per_date]
