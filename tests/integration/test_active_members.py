# test all_active members using the interaction matrix
import numpy as np
from discord_analyzer.analysis.assess_engagement import assess_engagement
from discord_analyzer.analysis.utils.activity import Activity

from .utils.activity_params import prepare_activity_params


def test_no_active():
    acc_names = []
    acc_count = 5
    for i in range(5):
        acc_names.append(f"user{i}")

    acc_names = np.array(acc_names)

    int_mat = {
        Activity.Reply: np.zeros((acc_count, acc_count)),
        Activity.Mention: np.zeros((acc_count, acc_count)),
        Activity.Reaction: np.zeros((acc_count, acc_count)),
    }

    activity_dict = {
        "all_joined": {},
        "all_joined_day": {"0": set()},
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

    # window index
    w_i = 0

    act_param = prepare_activity_params()

    (_, *computed_activities) = assess_engagement(
        int_mat=int_mat,
        w_i=w_i,
        acc_names=acc_names,
        act_param=act_param,
        WINDOW_D=WINDOW_D,
        **activity_dict,
    )

    computed_activities = dict(zip(activity_dict.keys(), computed_activities))

    assert computed_activities["all_joined"] == {}
    assert computed_activities["all_joined_day"] == {"0": set()}
    assert computed_activities["all_consistent"] == {"0": set()}
    assert computed_activities["all_vital"] == {"0": set()}
    assert computed_activities["all_active"] == {"0": set()}
    assert computed_activities["all_connected"] == {"0": set()}
    assert computed_activities["all_dropped"] == {"0": set()}
    assert computed_activities["all_new_active"] == {"0": set()}
    assert computed_activities["all_still_active"] == {"0": set()}
    assert computed_activities["all_paused"] == {"0": set()}
    assert computed_activities["all_returned"] == {"0": set()}
    assert computed_activities["all_unpaused"] == {"0": set()}
    assert computed_activities["all_new_disengaged"] == {"0": set()}
    assert computed_activities["all_disengaged"] == {"0": set()}
    assert computed_activities["all_disengaged_were_newly_active"] == {"0": set()}
    assert computed_activities["all_disengaged_were_consistently_active"] == {
        "0": set()
    }
    assert computed_activities["all_disengaged_were_vital"] == {"0": set()}
    assert computed_activities["all_lurker"] == {"0": set()}
    assert computed_activities["all_about_to_disengage"] == {"0": set()}
    assert computed_activities["all_disengaged_in_past"] == {"0": set()}


def test_single_active():
    acc_names = []
    acc_count = 5
    for i in range(5):
        acc_names.append(f"user{i}")

    acc_names = np.array(acc_names)

    int_mat = {
        Activity.Reply: np.zeros((acc_count, acc_count)),
        Activity.Mention: np.zeros((acc_count, acc_count)),
        Activity.Reaction: np.zeros((acc_count, acc_count)),
    }
    int_mat[Activity.Reply][0, 1] = 2

    activity_dict = {
        "all_joined": {"0": set()},
        "all_joined_day": {"0": set()},
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
    # time window
    WINDOW_D = 7
    # window index
    w_i = 0

    act_param = prepare_activity_params()

    (_, *computed_activities) = assess_engagement(
        int_mat=int_mat,
        w_i=w_i,
        acc_names=acc_names,
        act_param=act_param,
        WINDOW_D=WINDOW_D,
        **activity_dict,
    )

    computed_activities = dict(zip(activity_dict.keys(), computed_activities))

    assert computed_activities["all_joined"] == {"0": set()}
    assert computed_activities["all_consistent"] == {"0": set()}
    assert computed_activities["all_vital"] == {"0": set()}
    assert computed_activities["all_active"] == {"0": set(["user0", "user1"])}
    assert computed_activities["all_connected"] == {"0": set()}
    assert computed_activities["all_dropped"] == {"0": set()}
    assert computed_activities["all_new_active"] == {"0": set(["user0", "user1"])}
    assert computed_activities["all_still_active"] == {"0": set()}
    assert computed_activities["all_paused"] == {"0": set()}
    assert computed_activities["all_returned"] == {"0": set()}
    assert computed_activities["all_unpaused"] == {"0": set()}
    assert computed_activities["all_new_disengaged"] == {"0": set()}
    assert computed_activities["all_disengaged"] == {"0": set()}
    assert computed_activities["all_disengaged_were_newly_active"] == {"0": set()}
    assert computed_activities["all_disengaged_were_consistently_active"] == {
        "0": set()
    }
    assert computed_activities["all_disengaged_were_vital"] == {"0": set()}
    assert computed_activities["all_lurker"] == {"0": set()}
    assert computed_activities["all_about_to_disengage"] == {"0": set()}
    assert computed_activities["all_disengaged_in_past"] == {"0": set()}
