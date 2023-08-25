import numpy as np
from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity

from .utils.activity_params import prepare_activity_params


def test_disengaged_were_vital():
    acc_names = []
    acc_count = 10
    for i in range(acc_count):
        acc_names.append(f"user{i}")

    acc_names = np.array(acc_names)

    # four weeks
    max_interval = 42

    # preparing empty joined members dict
    all_joined = dict(
        zip(np.array(range(max_interval), dtype=str), np.repeat(set(), max_interval))
    )

    activity_dict = {
        "all_joined": {},
        "all_joined_day": all_joined,
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
    activities = activity_dict.keys()

    int_mat = {
        Activity.Reply: np.zeros((acc_count, acc_count)),
        Activity.Mention: np.zeros((acc_count, acc_count)),
        Activity.Reaction: np.zeros((acc_count, acc_count)),
    }
    # `user_0` intracting with `user_1`, `user_2`, `user_3`, `user_4`, `user_5`
    # at least 5 times was needed
    int_mat[Activity.Reaction][0, 1] = 6
    int_mat[Activity.Reaction][0, 2] = 6
    int_mat[Activity.Reaction][0, 3] = 6
    int_mat[Activity.Reaction][0, 4] = 6
    int_mat[Activity.Reaction][0, 5] = 6
    int_mat[Activity.Reaction][0, 6] = 6

    # the analytics
    for w_i in range(max_interval):
        # time window
        WINDOW_D = 7

        act_param = prepare_activity_params()

        (_, *activity_dict) = assess_engagement(
            int_mat=int_mat,
            w_i=w_i,
            acc_names=acc_names,
            act_param=act_param,
            WINDOW_D=WINDOW_D,
            **activity_dict,
        )

        activity_dict = dict(zip(activities, activity_dict))

        # zeroing all the activities for w_i == 22
        # we would have the all_disengaged_were_vital on day (22 + 7) = 29
        # we assumed -1 period as 7 days and the zeroth period would be day w_i = 29
        if w_i == 21:
            int_mat[Activity.Reaction][0, 1] = 0
            int_mat[Activity.Reaction][0, 2] = 0
            int_mat[Activity.Reaction][0, 3] = 0
            int_mat[Activity.Reaction][0, 4] = 0
            int_mat[Activity.Reaction][0, 5] = 0
            int_mat[Activity.Reaction][0, 6] = 0

    print("all_vital:", activity_dict["all_vital"])
    print("all_new_disengaged:", activity_dict["all_new_disengaged"])
    print("all_disengaged_were_vital", activity_dict["all_disengaged_were_vital"])

    assert activity_dict["all_disengaged_were_vital"] == {
        "0": set(),
        "1": set(),
        "2": set(),
        "3": set(),
        "4": set(),
        "5": set(),
        "6": set(),
        "7": set(),
        "8": set(),
        "9": set(),
        "10": set(),
        "11": set(),
        "12": set(),
        "13": set(),
        "14": set(),
        "15": set(),
        "16": set(),
        "17": set(),
        "18": set(),
        "19": set(),
        "20": set(),
        "21": set(),
        "22": set(),
        "23": set(),
        "24": set(),
        "25": set(),
        "26": set(),
        "27": set(),
        "28": set(),
        "29": set(),
        "30": set(),
        "31": set(),
        "32": set(),
        "33": set(),
        "34": set(),
        "35": {"user0"},
        "36": set(),
        "37": set(),
        "38": set(),
        "39": set(),
        "40": set(),
        "41": set(),
    }
