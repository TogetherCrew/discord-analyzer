import numpy as np
from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity

from .utils.activity_params import prepare_activity_params


def test_all_new_active():
    acc_names = []
    acc_count = 5
    for i in range(5):
        acc_names.append(f"user{i}")

    acc_names = np.array(acc_names)

    # four weeks
    max_interval = 28

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

    # `user_1` intracting with `user_2`
    int_mat[Activity.Reaction][0, 1] = 2

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
        # zeroing it on the day 7
        # we should have it all_disengaged_were_newly_active in day 7 + 7
        if w_i == 6:
            int_mat[Activity.Reaction][0, 1] = 0

    print("all_new_active:", activity_dict["all_new_active"])

    assert activity_dict["all_new_active"] == {
        "0": {"user0", "user1"},
        "1": {"user0", "user1"},
        "2": {"user0", "user1"},
        "3": {"user0", "user1"},
        "4": {"user0", "user1"},
        "5": {"user0", "user1"},
        "6": {"user0", "user1"},
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
    }
