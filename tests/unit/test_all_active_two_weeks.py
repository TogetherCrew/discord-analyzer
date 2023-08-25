# testing all_active category for past 2 weeks
import numpy as np

from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity


def test_all_active_two_weeks_index_schema():
    """
    test the all active category for past 2 weeks

    The interaction matrix is the represantative of window_d days
    so we should have the users as active for the interaction matrix
    and not the window_d period here
    """
    INT_THR = 1  # minimum number of interactions to be active
    UW_DEG_THR = 1  # minimum number of accounts interacted with to be active
    PAUSED_T_THR = 1  # time period to remain paused
    CON_T_THR = 4  # time period to be consistent active
    CON_O_THR = 3  # time period to be consistent active
    EDGE_STR_THR = 5  # minimum number of interactions for connected
    UW_THR_DEG_THR = 5  # minimum number of accounts for connected
    VITAL_T_THR = 4  # time period to assess for vital
    VITAL_O_THR = 3  # times to be connected within VITAL_T_THR to be vital
    STILL_T_THR = 2  # time period to assess for still active
    STILL_O_THR = 2  # times to be active within STILL_T_THR to be still active
    DROP_H_THR = 2
    DROP_I_THR = 1

    act_param = [
        INT_THR,
        UW_DEG_THR,
        PAUSED_T_THR,
        CON_T_THR,
        CON_O_THR,
        EDGE_STR_THR,
        UW_THR_DEG_THR,
        VITAL_T_THR,
        VITAL_O_THR,
        STILL_T_THR,
        STILL_O_THR,
        DROP_H_THR,
        DROP_I_THR,
    ]

    analytics_length = 14
    all_joined_day = dict(
        zip(
            np.array(range(analytics_length), dtype=str),
            np.repeat(set(), analytics_length),
        )
    )

    activity_dict = {
        "all_joined": {},
        "all_joined_day": all_joined_day,
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

    WINDOW_D = 7

    acc_names = np.array(
        [
            "user0",
            "user1",
            "user2",
            "user3",
        ]
    )
    acc_count = len(acc_names)

    int_mat = {
        Activity.Reply: np.zeros((acc_count, acc_count)),
        Activity.Mention: np.zeros((acc_count, acc_count)),
        Activity.Reaction: np.zeros((acc_count, acc_count)),
    }

    # from day zero user0 and user1 are active
    int_mat[Activity.Reaction][0, 1] = 1
    int_mat[Activity.Reaction][1, 0] = 2

    # two weeks represantative of 14 days
    for day_i in range(14):
        if day_i == 1:
            int_mat[Activity.Reaction][0, 1] = 0
            int_mat[Activity.Reaction][1, 0] = 0

        if day_i == 3:
            int_mat[Activity.Reaction][2, 3] = 2
            int_mat[Activity.Reaction][3, 2] = 4

        if day_i == 4:
            int_mat[Activity.Reaction][2, 3] = 0
            int_mat[Activity.Reaction][3, 2] = 0

        (_, *computed_activities) = assess_engagement(
            int_mat=int_mat,
            w_i=day_i,
            acc_names=acc_names,
            act_param=act_param,
            WINDOW_D=WINDOW_D,
            **activity_dict
        )

        computed_activities = dict(zip(activity_dict.keys(), computed_activities))
        activity_dict = computed_activities
    print(activity_dict["all_active"])

    assert activity_dict["all_active"] == {
        "0": {"user1", "user0"},
        "1": set(),
        "2": set(),
        "3": {"user2", "user3"},
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
    }
