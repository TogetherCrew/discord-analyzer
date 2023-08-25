import os

from dotenv import load_dotenv

from discord_analyzer import RnDaoAnalyzer
from discord_analyzer.analysis.assess_engagement import assess_engagement

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

    (graph, *computed_activities) = assess_engagement(
        int_mat=int_matrix,
        w_i=w_i,
        acc_names=acc_names,
        act_param=act_param,
        WINDOW_D=WINDOW_D,
        **activity_dict,
    )

    return graph


def store_mock_data_in_neo4j(graph_dict, guildId):
    # CREDS
    load_dotenv()
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASS")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")

    neo4j_creds = {}
    neo4j_creds["db_name"] = os.getenv("NEO4J_DB")
    neo4j_creds["protocol"] = os.getenv("NEO4J_PROTOCOL")
    neo4j_creds["host"] = os.getenv("NEO4J_HOST")
    neo4j_creds["port"] = os.getenv("NEO4J_PORT")
    neo4j_creds["password"] = os.getenv("NEO4J_PASSWORD")
    neo4j_creds["user"] = os.getenv("NEO4J_USER")

    analyzer = RnDaoAnalyzer()

    analyzer.set_mongo_database_info(
        mongo_db_host=host,
        mongo_db_password=password,
        mongo_db_user=user,
        mongo_db_port=port,
    )
    analyzer.set_neo4j_database_info(neo4j_creds=neo4j_creds)
    analyzer.database_connect()

    guilds_data = {}

    guilds_data[guildId] = {
        "heatmaps": None,
        "memberactivities": (
            None,
            graph_dict,
        ),
    }

    analyzer.DB_connections.store_analytics_data(
        analytics_data=guilds_data,
        remove_heatmaps=False,
        remove_memberactivities=False,
    )
