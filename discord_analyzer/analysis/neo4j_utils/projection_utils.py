import logging

import numpy as np
from graphdatascience import GraphDataScience


class ProjectionUtils:
    def __init__(self, gds: GraphDataScience, guildId: str) -> None:
        self.gds = gds
        self.guildId = guildId

    def project_temp_graph(
        self,
        guildId: str,
        graph_name: str,
        **kwargs,
    ) -> None:
        """
        project a temperory graph on the INTERACTED_WITH relations

        Parameters:
        ------------
        guildId : str
            the guildId we want to do the projection
        graph_name : str
            the name we want to name the projected graph
        **kwargs :
            weighted : bool
                whether to do the projection weighted or not
                default is False which means it doesn't include
                `weight` property of the graph
            relation_direction : str
                either `NATURAL`, `REVERSE`, `UNDIRECTED`
                default is `UNDIRECTED`
            projection_query : str
                the projection query for nodes `a` and `b` and the relation `r`
                default is
                    `MATCH (a:DiscordAccount)
                -[r:INTERACTED_WITH {{guildId: '{guildId}'}}]->
                (b:DiscordAccount)`
        """
        # getting kwargs
        weighted = False
        if "weighted" in kwargs:
            weighted = kwargs["weighted"]

        relation_direction = "UNDIRECTED"
        if "relation_direction" in kwargs:
            relation_direction = kwargs["relation_direction"]

        projection_query = f"""MATCH (a:DiscordAccount)
                -[r:INTERACTED_WITH {{guildId: '{guildId}'}}]->
                (b:DiscordAccount)  """
        if "projection_query" in kwargs:
            projection_query = kwargs["projection_query"]

        rel_direction = None
        if relation_direction == "NATURAL":
            # empty str
            rel_direction = ""
        elif relation_direction == "UNDIRECTED":
            rel_direction = ",{undirectedRelationshipTypes: ['*']}"
        elif relation_direction == "REVERSE":
            rel_direction = ",{inverseIndexedRelationshipTypes: ['*']}"
        else:
            logging.error("Wrong relation_direction given as input")
            logging.error(f"Given is: {relation_direction}, defaulting to UNDIRECTED")
            rel_direction = ",{undirectedRelationshipTypes: ['*']}"

        # initializing it
        rel_properties = None

        if weighted:
            # the relation properties to include
            rel_properties = "{.date, .weight}"
        else:
            rel_properties = "{.date}"

        _ = self.gds.run_cypher(
            f"""
            {projection_query}
            WITH gds.graph.project(
                "{graph_name}",
                a,
                b,
                {{
                    relationshipProperties: r {rel_properties}
                }}
                {rel_direction}
            ) AS g
            RETURN
            g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """
        )

    def project_subgraph_per_date(
        self, graph_name: str, subgraph_name: str, date: float
    ) -> None:
        """
        create a subgraph from a previously projected graph

        Parameters:
        ------------
        graph_name : str
            the projected graph name we wanted to get our subgraph
        subgraph_name : str
            the subgraph name we want to do the projections
        date : float
            timestamp we want to do the projection for
        """
        _ = self.gds.run_cypher(
            f"""
                    CALL gds.beta.graph.project.subgraph(
                        "{subgraph_name}",
                        "{graph_name}",
                        "*",
                        "r.date = $date",
                        {{
                            parameters: {{
                                date: {date}
                            }}
                        }}
                    )
                    """
        )

    def get_dates(self, guildId: str) -> set[float]:
        """
        get all the dates we do have on the INTERACTED_WITH relations

        Parameters:
        ------------
        guildId : str
            the guild we do want the dates of relations
        """
        dates = self.gds.run_cypher(
            f"""
            MATCH (a:DiscordAccount)
                -[r:INTERACTED_WITH {{guildId: '{guildId}'}}]-()
            WITH DISTINCT(r.date) as dates
            RETURN dates
            """
        )
        computable_dates_set = set(dates["dates"].values)

        return computable_dates_set

    def get_computed_dates(self, query: str) -> set[float]:
        """
        get the computed metric dates for that specific query

        Parameters:
        -------------
        query : str
            the query to get the computed dates of a metric
            must have two return results
            - first one is date
            - second one is the metric
        """
        computed_dates = self.gds.run_cypher(query)

        computed_dates = set(
            map(
                lambda x: None if np.isnan(x[1]) is np.bool_(True) else x[0],
                computed_dates.values,
            )
        )
        computed_dates = computed_dates - {None}

        return computed_dates
