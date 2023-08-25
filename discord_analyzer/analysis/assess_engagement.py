# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  assess_engagement.py
#
#  Author Ene SS Rawa / Tjitse van der Molen


# # # # # import libraries # # # # #

import copy as copy
from collections import Counter

import networkx as nx
import numpy as np

from discord_analyzer.analysis.utils.activity import Activity

# # # # # main function # # # # #


def assess_engagement(
    int_mat,
    w_i,
    acc_names,
    act_param,
    WINDOW_D,
    all_joined,
    all_joined_day,
    all_consistent,
    all_vital,
    all_active,
    all_connected,
    all_paused,
    all_new_disengaged,
    all_disengaged,
    all_unpaused,
    all_returned,
    all_new_active,
    all_still_active,
    all_dropped,
    all_disengaged_were_vital,
    all_disengaged_were_newly_active,
    all_disengaged_were_consistently_active,
    all_lurker,
    all_about_to_disengage,
    all_disengaged_in_past,
):
    """
    Assess engagment levels for all active members in a time period

    Input:
    graph - (graph, 1D np.array, 1D np.array) : (the graph object,
        weighted degree, fraction of weighted in degree)
    w_i - int : index of sliding time window
    WINDOW_D - int : duration of sliding window (days)
    all_* - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names belonging to engagement
        category *

    act_param - [int] : parameters for activity types:
    INT_THR - int : minimum number of interactions to be active
    UW_DEG_THR - int : minimum number of connections to be active
    EDGE_STR_THR - int : minimum number of interactions for connected
    UW_THR_DEG_THR - int : minimum number of accounts for connected
    CON_T_THR - int : time period to assess consistently active
    CON_O_THR - int : times to be active within CON_T_THR to be
        consistently active
    VITAL_T_THR - int : time period to assess for vital
    VITAL_O_THR - int : times to be connected within VITAL_T_THR to be vital
    PAUSED_T_THR - int : time period to remain paused
    STILL_T_THR - int : time period to assess for still active
    STILL_O_THR - int : times to be active within STILL_T_THR to be still active
    DROP_H_THR - int : time periods in the past to have been newly active
    DROP_I_THR - int : time periods to have been inactive

    Output:
    graph - {networkx object} : networkx object for int_mat
    all_* - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names belonging to engagement
        category * updated for window w_i
    all_disengaged_* - set : set with all unique account names belong to one of the
        disengaged member types for window w_i
    """

    # # # THRESHOLD INTERACTIONS # # #
    thr_ind, thr_uw_deg, thr_uw_thr_deg, graph = thr_int(
        int_mat, act_param[0], act_param[1], act_param[5], act_param[6]
    )

    # # # ACTIVE # # #

    all_active = assess_active(acc_names, thr_ind, thr_uw_deg, w_i, all_active)

    # # # # CONNECTED # # #

    all_connected = assess_connected(acc_names, thr_uw_thr_deg, w_i, all_connected)

    # # # CONSISTENTLY ACTIVE # # #

    all_consistent = assess_consistent(
        all_active, w_i, act_param[3], act_param[4], WINDOW_D, all_consistent
    )

    # # # VITAL # # #

    all_vital = assess_vital(
        all_connected, w_i, act_param[7], act_param[8], WINDOW_D, all_vital
    )

    # # # STILL ACTIVE # # #

    all_still_active = assess_still_active(
        all_new_active,
        all_active,
        w_i,
        act_param[9],
        act_param[10],
        WINDOW_D,
        all_still_active,
    )

    # # # DROPPED # # #

    all_dropped = assess_dropped(
        all_new_active,
        all_active,
        w_i,
        act_param[11],
        act_param[12],
        WINDOW_D,
        all_dropped,
    )

    # # # REMAINDER # # #

    (
        all_new_active,
        all_unpaused,
        all_returned,
        all_paused,
        all_new_disengaged,
        all_disengaged,
        all_disengaged_in_past,
    ) = assess_remainder(
        all_active,
        w_i,
        WINDOW_D,
        act_param[2],
        all_new_active,
        all_unpaused,
        all_returned,
        all_paused,
        all_new_disengaged,
        all_disengaged,
        all_disengaged_in_past,
    )

    # # # LURKER # # #

    all_lurker = assess_lurker(
        all_lurker,
        all_new_active,
        all_joined_day,
        w_i,
    )

    # # # ABOUT TO DISENGAGE # # #

    all_about_to_disengage[str(w_i)] = all_paused[str(w_i)] - all_consistent[str(w_i)]

    # # # SUBDIVIDE DISENGAGED TYPES # # #

    # make temporary dictionary for remaining disengaged members
    rem_new_disengaged = {}

    # if there is any disengagement data
    if str(w_i) in all_new_disengaged.keys():
        # assess who was core before they disengaged
        (
            rem_new_disengaged[str(w_i)],
            all_disengaged_were_vital[str(w_i)],
        ) = assess_overlap(
            all_new_disengaged, all_vital, str(w_i), (act_param[2] + 1) * WINDOW_D
        )

        # assess who of the remaining disengaged accounts
        # was consistently active before they disengaged
        (
            rem_new_disengaged[str(w_i)],
            all_disengaged_were_consistently_active[str(w_i)],
        ) = assess_overlap(
            rem_new_disengaged, all_consistent, str(w_i), (act_param[2] + 1) * WINDOW_D
        )

        # assess who of the remaining disengaged accounts
        #  was newly active before they disengaged
        (
            rem_new_disengaged[str(w_i)],
            all_disengaged_were_newly_active[str(w_i)],
        ) = assess_overlap(
            rem_new_disengaged, all_new_active, str(w_i), (act_param[2] + 1) * WINDOW_D
        )
    else:
        all_disengaged_were_vital[str(w_i)] = set()
        all_disengaged_were_consistently_active[str(w_i)] = set()
        all_disengaged_were_newly_active[str(w_i)] = set()

    return (
        graph,
        all_joined,
        all_joined_day,
        all_consistent,
        all_vital,
        all_active,
        all_connected,
        all_paused,
        all_new_disengaged,
        all_disengaged,
        all_unpaused,
        all_returned,
        all_new_active,
        all_still_active,
        all_dropped,
        all_disengaged_were_newly_active,
        all_disengaged_were_consistently_active,
        all_disengaged_were_vital,
        all_lurker,
        all_about_to_disengage,
        all_disengaged_in_past,
    )


# # # # # nested functions # # # # #


def thr_int(int_mat, INT_THR, UW_DEG_THR, EDGE_STR_THR, UW_THR_DEG_THR):
    """
    Computes number of interactions and connections per account

    Input:
    int_mat - [int] : 2D weighted directed interaction matrix
    INT_THR - int : minimum number of interactions to be active
    UW_DEG_THR - int : minimum number of connections to be active
    EDGE_STR_THR - int : minimum number of interactions for connected
    UW_THR_DEG_THR - int : minimum number of accounts for connected

    Output:
    thr_ind - [int] : index numbers of account names with at least
        INT_THR interactions
    thr_uw_deg - [int] : index numbers of account names with at least
        UW_DEG_THR connections
    thr_uw_thr_deg - [int] : index numbers of account names with at
        least UW_THR_DEG_THR connections of at least EDGE_STR_THR
        interactions each
    """

    # # # SELECT DATA FROM INT_MAT # # #

    # select number of active interactions per account
    int_analysis = (
        np.sum(int_mat[Activity.Reply], axis=0)
        + np.sum(int_mat[Activity.Reply], axis=1)
        + np.sum(int_mat[Activity.Reaction], axis=0)
        + np.sum(int_mat[Activity.Reaction], axis=1)
        + np.sum(int_mat[Activity.Mention], axis=1)
    )

    # turn int_mat from all interaction types into graph
    graph = make_graph(
        int_mat[Activity.Reply] + int_mat[Activity.Reaction] + int_mat[Activity.Mention]
    )

    # # # TOTAL INTERACTIONS # # #

    # compare total active interactions to active interaction threshold
    thr_ind = np.where(int_analysis >= INT_THR)[0]

    # # # TOTAL CONNECTIONS # # #

    # get unweighted node degree value for each node
    all_degrees = np.array([val for (node, val) in graph.degree()])

    # compare total unweighted node degree to interaction threshold
    thr_uw_deg = np.where(all_degrees >= UW_DEG_THR)[0]

    # # # THRESHOLDED CONNECTIONS # # #

    # make copy of graph for thresholding
    thresh_graph = copy.deepcopy(graph)

    # remove edges below threshold from copy
    thresh_graph.remove_edges_from(
        [
            (n1, n2)
            for n1, n2, w in thresh_graph.edges(data="weight")
            if w < EDGE_STR_THR
        ]
    )

    # get unweighted node degree value for each node from thresholded network
    all_degrees_thresh = np.array([val for (node, val) in thresh_graph.degree()])

    # compare total unweighted node degree after thresholding to threshold
    thr_uw_thr_deg = np.where(all_degrees_thresh > UW_THR_DEG_THR)[0]

    return [thr_ind, thr_uw_deg, thr_uw_thr_deg, graph]


# # #


def assess_active(acc_names, thr_ind, thr_uw_deg, w_i, all_active):
    """
    Assess all active accounts
    Input:
    acc_names - [str] : all active accounts in window
    thr_ind - [int] : index numbers of account names with at least
        INT_THR interactions
    thr_uw_deg - [int] : index numbers of account names with at least
        UW_DEG_THR connections
    w_i - int : index of sliding time window
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active
    Output:
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active updated
        for window w_i
    """

    # # obtain accounts that meet both weigthed and unweighted degree thresholds
    thr_overlap = np.intersect1d(thr_ind, thr_uw_deg)

    # obtain active account names in this period and store in dictionary
    all_active[str(w_i)] = set(acc_names[thr_overlap])

    return all_active


# # #


def assess_connected(acc_names, thr_uw_thr_deg, w_i, all_connected):
    """
    Assess all connected accounts
    Input:
    acc_names - [str] : all active accounts in window
    thr_uw_thr_deg - [int] : index numbers of account names with at
        least UW_THR_DEG_THR connections of at least EDGE_STR_THR
        interactions each
    w_i - int : index of sliding time window
    all_connected - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are connected
    Output:
    all_connected - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are connected updated
        for window w_i
    """

    # obtain connected account names in this period and store in dictionary
    all_connected[str(w_i)] = set(acc_names[thr_uw_thr_deg])

    return all_connected


# # #


def assess_consistent(all_active, w_i, CON_T_THR, CON_O_THR, WINDOW_D, all_consistent):
    """
    Assess all continuously active accounts
    Input:
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active
    w_i - int : index of sliding time window
    CON_T_THR - int : time period to assess consistently active
    CON_O_THR - int : times to be active within CON_T_THR to be
        consistently active
    WINDOW_D - int : duration of sliding window (days)
    n_consistent - [int] : list of number of accounts that are continuously active
    all_consistent - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are continuously active
    Output:
    all_consistent - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are consistently active updated
        for window w_i
    """

    # if there are more time periods in the past than CON_O_THR
    if w_i - (CON_O_THR - 1) * WINDOW_D >= 0:
        # obtain who was consistently active in all specified time periods
        all_consistent[str(w_i)] = set(
            check_past(all_active, CON_T_THR, CON_O_THR, WINDOW_D)
        )

    else:
        # store empty set
        all_consistent[str(w_i)] = set("")

    return all_consistent


# # #


def assess_vital(all_connected, w_i, VITAL_T_THR, VITAL_O_THR, WINDOW_D, all_vital):
    """
    Assess all vital accounts
    Input:
    all_connected - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are connected
    w_i - int : index of sliding time window
    VITAL_T_THR - int : time period to assess for vital
    VITAL_O_THR - int : times to be connected within VITAL_T_THR to be vital
    WINDOW_D - int : duration of sliding window (days)
    all_vital - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are vital

    Output:
    all_vital - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are vital updated
        for window w_i
    """

    # if there are more time periods in the past than CON_T_THR
    if w_i - VITAL_O_THR * WINDOW_D >= 0:
        # obtain who was connected in all specified time periods and was engaged
        all_vital[str(w_i)] = set(
            check_past(all_connected, VITAL_T_THR, VITAL_O_THR, WINDOW_D)
        )

    else:
        # store empty set
        all_vital[str(w_i)] = set("")

    return all_vital


# # #


def assess_still_active(
    all_new_active,
    all_active,
    w_i,
    STILL_T_THR,
    STILL_O_THR,
    WINDOW_D,
    all_still_active,
):
    """
    Assess all still active accounts
    Input:
    all_new_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active for first
         time in period
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active
    w_i - int : index of sliding time window
    STILL_T_THR - int : time period to assess for still active
    STILL_O_THR - int : times to be active within STILL_T_THR to be still active
    WINDOW_D - int : duration of sliding window (days)
    all_still_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are still active

    Output:
    all_still_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are still active
        updated for window w_i

    """

    # if there are more time periods in the past than STILL_T_THR
    if w_i - (STILL_T_THR * WINDOW_D) >= 0:
        # obtain who was active in sufficient specified time periods
        all_con_active = set(check_past(all_active, STILL_T_THR, STILL_O_THR, WINDOW_D))

        # select who of all_con_active were part of all arrived in period and store
        all_still_active[str(w_i)] = set(
            all_con_active.intersection(
                all_new_active[str(w_i - (STILL_T_THR * WINDOW_D))]
            )
        )

    else:
        # store empty set
        all_still_active[str(w_i)] = set("")

    return all_still_active


# # #


def assess_dropped(
    all_new_active, all_active, w_i, DROP_H_THR, DROP_I_THR, WINDOW_D, all_dropped
):
    """
    Assess all still active accounts
    Input:
    all_new_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active for first
         time in period
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active
    w_i - int : index of sliding time window
    DROP_H_THR - int : time periods in the past to have been newly active
    DROP_I_THR - int : time periods to have been inactive
    WINDOW_D - int : duration of sliding window (days)
    all_dropped - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are dropped

    Output:
    all_still_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are still active
        updated for window w_i

    """

    # if there are more time periods in the past than STILL_T_THR
    if w_i - (DROP_H_THR * WINDOW_D) >= 0:
        # obtain who was newly active in one of specified time periods
        all_new_per = set(check_past(all_new_active, DROP_H_THR, 1, WINDOW_D))

        # obtain who was active in one of the specified time periods
        all_act_per = set(check_past(all_active, DROP_I_THR, 1, WINDOW_D))

        # remove all_act_per from all_new_per and store results
        all_dropped[str(w_i)] = set(all_new_per - all_act_per)

    else:
        # store empty set
        all_dropped[str(w_i)] = set("")

    return all_dropped


# # #


def assess_remainder(
    all_active,
    w_i,
    WINDOW_D,
    PAUSED_T_THR,
    all_new_active,
    all_unpaused,
    all_returned,
    all_paused,
    all_new_disengaged,
    all_disengaged,
    all_disengaged_in_past,
):
    """
    Assess all remaing engagement categories
    Input:
    all_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active
    w_i - int : index of sliding time window
    WINDOW_D - int : duration of sliding window (days)
    PAUSED_T_THR - int : time period to remain paused
    n_* - [int] : list of number of accounts that are *
    all_* - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are *

    Output:
    all_* - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are * updated
        for window w_i
    """

    # if data from previous period is available
    if (w_i - WINDOW_D >= 0) and (str(w_i - WINDOW_D) in all_active.keys()):
        # check if there is paused data from previous period
        #  and otherwise make empty set
        temp_set_paused = check_prev_period(all_paused, str(w_i - WINDOW_D))

        # check if there is disengaged data from
        #  previous period and otherwise make empty set
        temp_set_disengaged = check_prev_period(all_disengaged, str(w_i - WINDOW_D))

        # check if there is unpaused data from previous period and
        #  otherwise make empty set
        temp_set_unpaused = check_prev_period(all_unpaused, str(w_i - WINDOW_D))

        # # # NEWLY ACTIVE # # #

        # obtain members active in this window that were not active,
        #  paused or disengaged WINDOW_D days ago
        all_new_active[str(w_i)] = (
            set(all_active[str(w_i)])
            - set(all_active[str(w_i - WINDOW_D)])
            - temp_set_paused
            - temp_set_disengaged
            - temp_set_unpaused
        )

        # # # PAUSED (1 of 2)# # #

        # obtain members that were active WINDOW_D days ago
        #  but are not active in this window
        new_paused = set(all_active[str(w_i - WINDOW_D)]) - set(all_active[str(w_i)])

        # add newly paused members to paused members from previous period
        temp_currently_paused = new_paused.union(temp_set_paused)

        # create temporary empty set result (will be updated in part 2 of 2)
        all_paused[str(w_i)] = set("")

        # if data from previous previous period is available
        if w_i - 2 * WINDOW_D >= 0:
            # # # UNPAUSED # # #

            # obtain account names active now but paused WINDOW_D days ago
            all_unpaused[str(w_i)] = set(all_paused[str(w_i - WINDOW_D)]).intersection(
                all_active[str(w_i)]
            )

            # remove unpaused from currently paused
            temp_currently_paused = temp_currently_paused - all_unpaused[str(w_i)]

            # # # RETURNED # # #

            # if there is disengaged data for this time period
            if str(w_i - WINDOW_D) in all_disengaged.keys():
                # obtain account names active now but disengaged WINDOW_D days ago
                all_returned[str(w_i)] = set(
                    all_disengaged[str(w_i - WINDOW_D)]
                ).intersection(all_active[str(w_i)])

            else:
                # store empty set for returned
                all_returned[str(w_i)] = set("")

            # # # DISENGAGED # # #

            # obtain account names that were continuously
            #  paused for PAUSED_T_THR periods
            cont_paused = check_past(
                all_paused, PAUSED_T_THR + 1, PAUSED_T_THR, WINDOW_D
            )

            # obtain account names that were
            #  continuously paused and are still not active
            all_new_disengaged[str(w_i)] = set(
                cont_paused.intersection(temp_currently_paused)
            )

            # add newly disengaged members to disengaged members
            #  from previous period
            temp_currently_disengaged = all_new_disengaged[str(w_i)].union(
                temp_set_disengaged
            )

            # remove returned accounts from disengaged accounts and store
            all_disengaged[str(w_i)] = set(
                temp_currently_disengaged - all_returned[str(w_i)]
            )

            # store who disengaged in the past
            all_disengaged_in_past[str(w_i)] = (
                all_disengaged[str(w_i)] - all_new_disengaged[str(w_i)]
            )

            # remove disengaged accounts from paused accounts
            temp_currently_paused = temp_currently_paused - all_disengaged[str(w_i)]

        # # # PAUSED (2 of 2) # # #

        # store currently paused accounts
        all_paused[str(w_i)] = set(temp_currently_paused)

    else:
        # set all active members to newly active
        all_new_active[str(w_i)] = set(all_active[str(w_i)])

        # set remaining activity types to empty string
        all_paused[str(w_i)] = set("")
        all_unpaused[str(w_i)] = set("")
        all_returned[str(w_i)] = set("")
        all_new_disengaged[str(w_i)] = set("")
        all_disengaged[str(w_i)] = set("")
        all_disengaged_in_past[str(w_i)] = set("")

    return [
        all_new_active,
        all_unpaused,
        all_returned,
        all_paused,
        all_new_disengaged,
        all_disengaged,
        all_disengaged_in_past,
    ]


# # #


def assess_lurker(
    all_lurker,
    all_new_active,
    all_joined_day,
    w_i,
):
    """
    Assess all lurker accounts
    Input:
    all_lurker - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are lurkers
    all_new_active - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are active for first
         time in period
    all_joined_day - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that joined on w_i
    w_i - int : index of sliding time window

    Output:
    all_lurker - {str : [str]} : dictionary with keys w_i and values
        containing a list of all account names that are lurkers
        updated for window w_i
    """
    # if data for previous period exists
    if w_i >= 1:
        # combine lurker from previous period with newly joined from this period
        temp_lurker = set(all_lurker[str(w_i - 1)]).union(set(all_joined_day[str(w_i)]))

    # if this is the first period
    else:
        # store all joined accounts as temp_lurkers
        data = []
        for member in all_joined_day[str(w_i)]:
            if member not in data:
                data.append(member)
        temp_lurker = set(data)

    # remove newly active accounts from temp_lurker and store
    all_lurker[str(w_i)] = temp_lurker - all_new_active[str(w_i)]

    return all_lurker


# # #


def assess_overlap(ref_dict, comp_dict, w_i, num_past):
    # define comparison period
    comp_per = int(w_i) - num_past

    # if comparison period is present in keys
    if str(comp_per) in comp_dict.keys():
        # assess overlap
        overlap_acc = set(ref_dict[w_i]).intersection(set(comp_dict[str(comp_per)]))

        # store remaining accounts
        rem_acc = set(ref_dict[w_i]) - overlap_acc

    else:
        # store empty set
        overlap_acc = set("")

        # set remaining accounts to all initial accounts
        rem_acc = set(ref_dict[w_i])

    return [rem_acc, overlap_acc]


# # #


def check_prev_period(engagement_dict, time_str):
    """
    Checks if values are present in specific previous period of dict

    Input:
    engagement_dict - {str : set} : dictionary with account names sets
        as values for periods indicated as keys
    time_str - str : dictionary key of interest

    Output:
    temp_set - set : either the set that is the value for the time_str
        key or and empty set
    """

    # if engagement_dict contains data for time_str
    if time_str in engagement_dict.keys():
        temp_set = set(engagement_dict[time_str])
    else:
        temp_set = set("")

    return temp_set


# # #


def check_past(data_dic, t_thr, o_thr, WINDOW_D):
    """
    Checks in how many previous periods account names were in a dict

    Input:
    data_dic - {str : set} : dictionary with account name sets to check
    t_thr - int : number of time period into the past to consider
    o_thr - int : minimal number of occurences of account name within
        the period specified by t_thr
    WINDOW_D - int : width of an analysis window in number of days

    Output:
    acc_selection - [str] : all accounts that were present in data_dic
        for more than o_thr times within the last t_thr periods
    """

    # initiate empty result list
    acc_per_period = [None] * t_thr

    # obtain dictionary keys
    dic_keys = list(data_dic.keys())

    # for each period that should be considered
    for p in range(t_thr):
        # if time period is present in dic_keys
        if len(dic_keys) >= -(-1 - (p * WINDOW_D)):
            # obtain accounts in period
            acc_per_period[p] = list(data_dic[str(dic_keys[-1 - (p * WINDOW_D)])])

        else:
            # store empty values
            acc_per_period[p] = list("")

    # merge values in list of list into single list
    all_acc_list = [elem for sublist in acc_per_period for elem in sublist]

    # count number of occurences in list per account
    acc_cnt_dict = Counter(all_acc_list)

    # obtain account names that with at least o_thr occurences in all_acc_list
    acc_selection = set(
        [acc for acc, occurrences in acc_cnt_dict.items() if occurrences >= o_thr]
    )

    return acc_selection


# # #


def make_graph(mat):
    """
    Turns interaction matrix into a directed graph object

    Input:
    mat - np.array : interaction matrix

    Output:
    graph - graph object: interaction graph
    """

    ###### The commented codes were written for creating undirected graph
    # # make empty result matrix (for undirected matrix)
    # new_mat = np.zeros_like(mat)

    # # for each row
    # for r in range(np.shape(mat)[0]):

    #     # for each column
    #     for c in range(r):

    #         # sum (r,c) and (c,r) values and store
    #         new_mat[r, c] = mat[r, c] + mat[c, r]

    # # turn matrix into graph
    # graph = nx.from_numpy_array(new_mat)

    # turn into directed graph
    graph = nx.from_numpy_array(mat, create_using=nx.DiGraph)

    return graph
