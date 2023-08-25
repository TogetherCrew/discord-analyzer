import numpy as np
from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity

from .utils.activity_params import prepare_activity_params


def test_disengaged_members():
    acc_names = []
    acc_count = 10
    for i in range(acc_count):
        acc_names.append(f"user{i}")

    acc_names = np.array(acc_names)

    # four weeks
    max_interval = 50

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

        # zeroing all the activities on day 29
        # meaning we could have disengaged members on day (29 + 7) = 36
        # 14 is two periods
        if w_i == 28:
            int_mat[Activity.Reaction][0, 1] = 0

    # BUG: we're getting empty for this -> output is {}
    print("all_active", activity_dict["all_active"])
    print("all_disengaged", activity_dict["all_disengaged"])

    assert activity_dict["all_disengaged"] == {
        "0": set(),
        "1": set(),
        "2": set(),
        "3": set(),
        "4": set(),
        "5": set(),
        "6": set(),
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
        "35": set(),
        "36": {"user1", "user0"},
        "37": {"user1", "user0"},
        "38": {"user1", "user0"},
        "39": {"user1", "user0"},
        "40": {"user1", "user0"},
        "41": {"user1", "user0"},
        "42": {"user1", "user0"},
        "43": {"user1", "user0"},
        "44": {"user1", "user0"},
        "45": {"user1", "user0"},
        "46": {"user1", "user0"},
        "47": {"user1", "user0"},
        "48": {"user1", "user0"},
        "49": {"user1", "user0"},
    }
