# Store and Rietrive the network graph from neo4j db

import datetime

import networkx
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib import Query


class NetworkGraph:
    def __init__(
        self,
        graph_schema: GraphSchema,
        platform_id: str,
    ) -> None:
        self.graph_schema = graph_schema
        self.platform_id = platform_id

    def make_neo4j_networkx_query_dict(
        self,
        networkx_graphs: dict[datetime.datetime, networkx.classes.graph.Graph],
    ) -> list[Query]:
        """
        make a list of queries to store networkx graphs into the neo4j

        Parameters:
        -------------
        networkx_graphs : dictionary of networkx.classes.graph.Graph
                            or networkx.classes.digraph.DiGraph
            the dictinoary keys is the date of graph and the values
            are the actual networkx graphs

        Returns:
        -----------
        queries_list : list[Query]
            list of string queries to store data into neo4j
        """
        # extract the graphs and their corresponding interaction dates
        graph_list, graph_dates = list(networkx_graphs.values()), list(
            networkx_graphs.keys()
        )

        # make a list of queries for each date to save
        queries_list = self.make_graph_list_query(
            networkx_graphs=graph_list,
            networkx_dates=graph_dates,
        )

        return queries_list

    def make_graph_list_query(
        self,
        networkx_graphs: networkx.classes.graph.Graph,
        networkx_dates: list[datetime.datetime],
    ) -> list[Query]:
        """
        Make a list of queries for each graph to save their results

        Parameters:
        -------------
        networkx_graphs : list of networkx.classes.graph.Graph
                        or networkx.classes.digraph.DiGraph
            the list of graph created from user interactions
        networkx_dates : list of dates
            the dates for each graph


        Returns:
        ---------
        final_queries : list[Query]
            list of strings, each is a query for an interaction graph to be created
        """
        final_queries: list[Query] = []

        for graph, date in zip(networkx_graphs, networkx_dates):
            nodes_dict = graph.nodes.data()
            edges_dict = graph.edges.data()

            node_queries, query_relations = self.create_network_query(
                nodes_dict,
                edges_dict,
                date,
            )

            final_queries.extend(node_queries)
            final_queries.extend(query_relations)

        return final_queries

    def create_network_query(
        self,
        nodes_dict: networkx.classes.reportviews.NodeDataView,
        edge_dict: networkx.classes.reportviews.EdgeDataView,
        graph_date: datetime.datetime,
    ) -> tuple[list[Query], list[Query]]:
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

        Returns:
        ----------
        node_queries : list[Query]
            the list of MERGE queries for creating all nodes
        rel_queries : list[Query]
            the list of MERGE queries for creating all relationships
        """
        # getting the timestamp `date`
        graph_date_timestamp = self.get_timestamp(graph_date)
        date_now_timestamp = self.get_timestamp()

        # labels to be saved in Neo4j
        # i.e.: DiscordMember
        user_label = self.graph_schema.user_label
        # i.e.: DiscordPlatform
        platform_label = self.graph_schema.platform_label
        member_rel_label = self.graph_schema.member_relation
        users_rel_label = self.graph_schema.interacted_with_rel

        # initializiation of queries
        rel_queries: list[Query] = []
        node_queries: list[Query] = []

        for node in nodes_dict:
            node_str_query = ""
            # retrieving node data
            # user number
            node_num = node[0]
            # user account name
            node_acc_name = node[1]["acc_name"]
            # creating the query
            node_str_query += (
                f"MERGE (a{node_num}:{user_label} {{id: $node_acc_name}})   "
            )
            node_str_query += f"""ON CREATE SET a{node_num}.createdAt =
                                        $date_now_timestamp
                                """

            # creating the platform if they weren't created before
            node_str_query += f"""MERGE (g:{platform_label} {{id: $platform_id}})
                                ON CREATE SET g.createdAt = $date_now_timestamp
                            """

            node_str_query += f"""
                MERGE (a{node_num})
                        -[rel_platform{node_num}:{member_rel_label}]-> (g)
                    ON CREATE SET
                        rel_platform{node_num}.createdAt = $date_now_timestamp
            """

            parameters = {
                "node_acc_name": node_acc_name,
                "date_now_timestamp": int(date_now_timestamp),
                "platform_id": self.platform_id,
            }
            query_str = node_str_query + ";"

            node_queries.append(Query(query_str, parameters))

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

            rel_str_query += f"""MATCH (a{starting_acc_num}:{user_label}
                                {{id: $starting_node_acc_name}})
                                    MATCH (a{ending_acc_num}:{user_label}
                                    {{id: $ending_node_acc_name}})
                                    MERGE
                                    (a{starting_acc_num}) -[rel{idx}:{users_rel_label}
                                        {{
                                            date: $date,
                                            weight: $weight,
                                            platformId: $platform_id
                                        }}
                                    ]-> (a{ending_acc_num})
                                        """
            query_str = rel_str_query + ";"
            parameters = {
                "starting_node_acc_name": starting_node_acc_name,
                "ending_node_acc_name": ending_node_acc_name,
                "date": int(graph_date_timestamp),
                "weight": int(interaction_count),
                "platform_id": self.platform_id,
            }
            rel_queries.append(Query(query_str, parameters))

        return node_queries, rel_queries

    def get_timestamp(self, time: datetime.datetime | None = None) -> float:
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
