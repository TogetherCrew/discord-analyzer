# Store and Rietrive the network graph from neo4j db

import datetime

import networkx


def make_neo4j_networkx_query_dict(
    networkx_graphs: dict[networkx.classes.graph.Graph],
    guildId: str,
    community_id: str,
):
    """
    make a list of queries to store networkx graphs into the neo4j

    Parameters:
    -------------
    networkx_graphs : dictionary of networkx.classes.graph.Graph
                        or networkx.classes.digraph.DiGraph
        the dictinoary keys is the date of graph and the values
         are the actual networkx graphs
    guildId : str
        the guild that the members belong to
    community_id : str
        the community id to save the data for

    Returns:
    -----------
    queries_list : list
        list of string queries to store data into neo4j
    """
    # extract the graphs and their corresponding interaction dates
    graph_list, graph_dates = list(networkx_graphs.values()), list(
        networkx_graphs.keys()
    )

    # make a list of queries for each date to save
    # the Useraccount and INTERACTED relation between them
    queries_list = make_graph_list_query(
        networkx_graphs=graph_list,
        networkx_dates=graph_dates,
        guildId=guildId,
        community_id=community_id,
        toGuildRelation="IS_MEMBER",
    )

    return queries_list


def make_graph_list_query(
    networkx_graphs: networkx.classes.graph.Graph,
    networkx_dates: list[datetime.datetime],
    guildId: str,
    community_id: str,
    toGuildRelation: str = "IS_MEMBER",
):
    """
    Make a list of queries for each graph to save their results

    Parameters:
    -------------
    networkx_graphs : list of networkx.classes.graph.Graph
                      or networkx.classes.digraph.DiGraph
        the list of graph created from user interactions
    networkx_dates : list of dates
        the dates for each graph
    guildId : str
        the guild that the members belong to
        default is `None` meaning that it wouldn't be belonged to any guild
    community_id : str
        the community id to save the data for
    toGuildRelation : str
        the relationship label that connect the users to guilds
        default value is `IS_MEMBER`

    Returns:
    ---------
    final_queries : list of str
        list of strings, each is a query for an interaction graph to be created
    """
    final_queries = []

    for graph, date in zip(networkx_graphs, networkx_dates):
        nodes_dict = graph.nodes.data()
        edges_dict = graph.edges.data()

        node_queries, query_relations = create_network_query(
            nodes_dict,
            edges_dict,
            date,
            guildId=guildId,
            toGuildRelation=toGuildRelation,
        )
        community_query = create_community_node_query(community_id, guildId)

        final_queries.extend(node_queries)
        final_queries.extend(query_relations)
        final_queries.append(community_query)

    return final_queries


def create_community_node_query(
    community_id: str,
    guild_id: str,
    community_node: str = "Community",
) -> str:
    """
    create the community node

    Parameters
    ------------
    community_id : str
        the community id to create its node
    guild_id : str
        the guild node to attach to community
    """
    date_now_timestamp = get_timestamp()

    query = f"""
        MERGE (g:Guild {{guildId: '{guild_id}'}})
        ON CREATE SET g.createdAt = {int(date_now_timestamp)}
        WITH g
        MERGE (c:{community_node} {{id: '{community_id}'}})
        ON CREATE SET c.createdAt = {int(date_now_timestamp)}
        WITH g, c
        MERGE (g) -[r:IS_WITHIN]-> (c)
        ON CREATE SET r.createdAt = {int(date_now_timestamp)}
    """

    return query


def create_network_query(
    nodes_dict: networkx.classes.reportviews.NodeDataView,
    edge_dict: networkx.classes.reportviews.EdgeDataView,
    graph_date: datetime.datetime,
    guildId: str,
    nodes_type: str = "DiscordAccount",
    rel_type: str = "INTERACTED_WITH",
    toGuildRelation: str = "IS_MEMBER",
):
    """
    make string query to save the accounts with their
     account_name and relationships with their relation from **a graph**.
    The query to add the nodes and edges is using `MERGE` operator
     of Neo4j db since it won't create duplicate nodes and edges
     if the relation and the account was saved before

    Parameters:
    -------------
    nodes_dict : NodeDataView
        the nodes of a Networkx graph
    edge_dict : EdgeDataView
        the edges of a Networkx graph
    graph_date : datetime
        the date of the interaction in as a python datetime object
    nodes_type : str
        the type of nodes to be saved
        default is `Account`
    rel_type : str
        the type of relationship to create
        default is `INTERACTED`

    Returns:
    ----------
    node_queries : list of str
        the list of MERGE queries for creating all nodes
    rel_queries : list of str
        the list of MERGE queries for creating all relationships
    """
    # getting the timestamp `date`
    graph_date_timestamp = get_timestamp(graph_date)
    date_now_timestamp = get_timestamp()

    # initializiation of queries
    rel_queries = []
    node_queries = []

    for node in nodes_dict:
        node_str_query = ""
        # retrieving node data
        # user number
        node_num = node[0]
        # user account name
        node_acc_name = node[1]["acc_name"]
        # creating the query
        node_str_query += (
            f"MERGE (a{node_num}:{nodes_type} {{userId: '{node_acc_name}'}})   "
        )
        node_str_query += f"""ON CREATE SET a{node_num}.createdAt =
                                    {int(date_now_timestamp)}
                            """

        # relationship query between users and guilds
        if guildId is not None:
            # creating the guilds if they weren't created before
            node_str_query += f"""MERGE (g:Guild {{guildId: '{guildId}'}})
                                ON CREATE SET g.createdAt = {int(date_now_timestamp)}
                            """

            node_str_query += f"""
                MERGE (a{node_num})
                        -[rel_guild{node_num}:{toGuildRelation}]-> (g)
                    ON CREATE SET
                        rel_guild{node_num}.createdAt = {int(date_now_timestamp)}
            """

        node_queries.append(node_str_query + ";")

    for idx, edge in enumerate(edge_dict):
        rel_str_query = ""

        # retrieving edge data

        # relationship from user number
        starting_acc_num = edge[0]
        # relationship to user number
        ending_acc_num = edge[1]

        starting_node_acc_name = nodes_dict[starting_acc_num]["acc_name"]
        ending_node_acc_name = nodes_dict[ending_acc_num]["acc_name"]

        # the interaction count between them
        interaction_count = edge[2]["weight"]

        rel_str_query += f"""MATCH (a{starting_acc_num}:{nodes_type}
                            {{userId: \'{starting_node_acc_name}\'}})
                                MATCH (a{ending_acc_num}:{nodes_type}
                                  {{userId: \'{ending_node_acc_name}\'}})
                                MERGE
                                (a{starting_acc_num}) -[rel{idx}:{rel_type}
                                    {{
                                        date: {int(graph_date_timestamp)},
                                        weight: {int(interaction_count)},
                                        guildId: '{guildId}'
                                    }}
                                ]-> (a{ending_acc_num})
                                    """
        rel_queries.append(rel_str_query + ";")

    return node_queries, rel_queries


def get_timestamp(time: datetime.datetime | None = None) -> float:
    """
    get the timestamp of the given time or just now

    Parameters
    ------------
    time : datetime.datetime
        the time to get its timestamp
        default is `None` meaning to send the time of now

    Returns
    --------
    timestamp : float
        the timestamp of the time multiplied to 1000
    """
    using_time: datetime.datetime
    if time is not None:
        using_time = time
    else:
        using_time = datetime.datetime.now()

    timestamp = (
        using_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        ).timestamp()
        * 1000
    )

    return timestamp
