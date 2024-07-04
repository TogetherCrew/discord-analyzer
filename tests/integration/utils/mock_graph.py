# import os

# from dotenv import load_dotenv
# from tc_analyzer_lib.tc_analyzer import TCAnalyzer
from tc_core_analyzer_lib.assess_engagement import EngagementAssessment
from tc_core_analyzer_lib.utils.activity import DiscordActivity

from .activity_params import prepare_activity_params


def generate_mock_graph(int_matrix, acc_names):
    # preparing some parameters
    act_param = prepare_activity_params()

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

    assess_engagment = EngagementAssessment(
        activities=[
            DiscordActivity.Mention,
            DiscordActivity.Reply,
            DiscordActivity.Reaction,
        ],
        activities_ignore_0_axis=[DiscordActivity.Mention],
        activities_ignore_1_axis=[],
    )

    (graph, *computed_activities) = assess_engagment.compute(
        int_mat=int_matrix,
        w_i=w_i,
        acc_names=acc_names,
        act_param=act_param,
        WINDOW_D=WINDOW_D,
        **activity_dict,
    )

    return graph


# def store_mock_data_in_neo4j(graph_dict, guildId, community_id):
#     # CREDS
#     load_dotenv()

#     analyzer = TCAnalyzer(guildId)
#     analyzer.database_connect()

#     guilds_data = {}

#     guilds_data["heatmaps"] = None
#     guilds_data["memberactivities"] = (
#         None,
#         graph_dict,
#     )

#     analyzer.DB_connections.store_analytics_data(
#         analytics_data=guilds_data,
#         guild_id=guildId,
#         community_id=community_id,
#         remove_heatmaps=False,
#         remove_memberactivities=False,
#     )
