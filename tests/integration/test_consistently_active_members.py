import numpy as np

from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity

from .utils.activity_params import prepare_activity_params


def test_two_consistently_active():
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

    print("all_consistent:", activity_dict["all_consistent"])
    print("all_vital:", activity_dict["all_vital"])

    assert activity_dict["all_consistent"] == {
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
        # index 14 == Day 15
        # Starting day of week 3
        "14": {"user0", "user1"},
        "15": {"user0", "user1"},
        "16": {"user0", "user1"},
        "17": {"user0", "user1"},
        "18": {"user0", "user1"},
        "19": {"user0", "user1"},
        "20": {"user0", "user1"},
        "21": {"user0", "user1"},
        "22": {"user0", "user1"},
        "23": {"user0", "user1"},
        "24": {"user0", "user1"},
        "25": {"user0", "user1"},
        "26": {"user0", "user1"},
        "27": {"user0", "user1"},
    }
