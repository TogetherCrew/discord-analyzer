import logging

from tc_neo4j_lib.neo4j_ops import Neo4jOps


class ProjectionUtils:
    def __init__(self, guildId: str) -> None:
        self.gds = Neo4jOps.get_instance().gds
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
            date : float
                if we want to include date in the graph projection query
        """
        # getting kwargs
        weighted = False
        if "weighted" in kwargs:
            weighted = kwargs["weighted"]

        relation_direction = "UNDIRECTED"
        if "relation_direction" in kwargs:
            relation_direction = kwargs["relation_direction"]

        projection_query: str
        if "date" in kwargs:
            date = kwargs["date"]
            projection_query = f"""MATCH (a:DiscordAccount)
                   -[r:INTERACTED_WITH {{guildId: '{guildId}', date: {date}}}]->
                   (b:DiscordAccount)  """
        else:
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

    def get_computed_dates(self, query: str, **params) -> set[float]:
        """
        get the computed metric dates for that specific query

        Parameters:
        -------------
        query : str
            the query to get the computed dates of a metric
            must have one return results with label of computed_dates
            first one is date
        params: Dict[str, Any]
            parameters to the query
        """
        dates = self.gds.run_cypher(query, params)
        computed_dates = set(dates["computed_dates"].values)

        return computed_dates
