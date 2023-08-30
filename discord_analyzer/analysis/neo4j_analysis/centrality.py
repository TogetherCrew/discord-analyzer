import logging

import pandas as pd
from discord_analyzer.analysis.neo4j_metrics import Neo4JMetrics
from discord_analyzer.analysis.neo4j_utils.projection_utils import ProjectionUtils
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class Centerality:
    def __init__(self, neo4j_ops: Neo4jOps) -> None:
        """
        centerality algorithms
        """
        self.neo4j_ops = neo4j_ops

    def compute_degree_centerality(
        self,
        guildId: str,
        direction: str,
        from_start: bool,
        **kwargs,
    ) -> dict[float, dict[str, float]]:
        """
        compute the weighted count of edges coming to a node
        it would be based on the date
        the computed_dates will be based on the
        network decentrality metric computations

        Parameters:
        ------------
        guildId : str
            the user nodes of guildId
        gds : GraphDataScience
            the gds instance to interact with DB
        direction : str
            the direction of relation
            could be `in_degree`, `out_degree`, `undirected`
        from_start : bool
            whether to compute everything from scratch
            or continue the computations
        kwargs : dict
            node : str
                the name of the node we're computing degree centrality
                default is `DiscordAccount`
            weighted : bool
                assuming the edges as weighted or not
                default is `True`
            normalize : bool
                whether to normalize the values or not
                default is False, meaning values wouldn't be normalized
            preserve_parallel : bool
                preserve parallel relationships
                or do not count 2 the parallel relations
                default is `True` which means we do
                count the parallel relationships as 2

                Never use `preserve_parallel=True` with `weighted=True` because
                it could produce wrong results, since we cannot sum weights with
                parallel relationships
            recompute_dates : set[datetime.timestamp]
                the dates that must be included in computations
                in another words, recompute analytics for that date

        Returns:
        ----------
        degree_centerality : dict[float, dict[str, float]]
            the degree centerality per date for each user
            the `float` keys are representative of the timestamp date
            the `str` is representative userId
            and the last `float` is represantative of user centrality value
        """

        node = "DiscordAccount" if "node" not in kwargs.keys() else kwargs["node"]
        weighted = True if "weighted" not in kwargs.keys() else kwargs["weighted"]
        normalize = False if "normalize" not in kwargs.keys() else kwargs["normalize"]
        preserve_parallel = (
            True
            if "preserve_parallel" not in kwargs.keys()
            else kwargs["preserve_parallel"]
        )

        recompute_dates = None
        if "recompute_dates" in kwargs:
            recompute_dates = kwargs["recompute_dates"]

        if weighted and not preserve_parallel:
            logging.warn(
                """preserver_parallel=False with weighted=True
                could produce wrong results!"""
            )

        # determining one line of the query useing the direction variable
        if direction == "in_degree":
            query = f"MATCH (a:{node})<-[r:INTERACTED_WITH]-(b:{node})"
        elif direction == "out_degree":
            query = f"MATCH (a:{node})-[r:INTERACTED_WITH]->(b:{node})"
        elif direction == "undirected":
            query = f"MATCH (a:{node})-[r:INTERACTED_WITH]-(b:{node})"

        results = self.neo4j_ops.gds.run_cypher(
            f"""
                {query}
                WHERE r.guildId = '{guildId}'
                RETURN
                    a.userId as a_userId,
                    r.date as date,
                    r.weight as weight,
                    b.userId as b_userId
            """
        )

        dates_to_compute = set(results["date"].value_counts().index)
        if not from_start:
            projection_utils = ProjectionUtils(gds=self.neo4j_ops.gds, guildId=guildId)

            dates_to_compute = self._get_dates_to_compute(
                projection_utils, dates_to_compute, guildId
            )
            if recompute_dates is not None:
                dates_to_compute = dates_to_compute.union(recompute_dates)

        degree_centerality = self.count_degrees(
            computation_date=dates_to_compute,
            results=results,
            weighted=weighted,
            normalize=normalize,
            preserve_parallel=preserve_parallel,
        )

        return degree_centerality

    def _get_dates_to_compute(
        self,
        projection_utils: ProjectionUtils,
        user_interaction_dates: set[float],
        guildId: str,
    ) -> set[float]:
        """
        exclude available analyzed date

        Parameters:
        -------------
        user_interaction_dates : set[float]
            the date of interactions between users
        guildId : str
            the guildId to get computations date
        """
        query = f"""
            MATCH (g:Guild {{guildId: '{guildId}'}})
                -[r:HAVE_METRICS] -> (g)
            RETURN r.date as date, r.decentralizationScore as dc
            """
        computed_dates = projection_utils.get_computed_dates(query)

        dates_to_compute = user_interaction_dates - computed_dates

        return dates_to_compute

    def count_degrees(
        self,
        computation_date: set[float],
        results: pd.DataFrame,
        weighted: bool,
        normalize: bool,
        preserve_parallel: bool,
    ) -> dict[float, dict[str, float]]:
        """
        count the degree of nodes
        (the direction of the relation depends on the results)

        Parameters:
        -------------
        results : pd.DataFrame
            the results for userId, `interaction_date`, and `weight` of relations
        computation_date : set[float]
            the dates to compute the analytics
        weighted : bool
            whether to use the weights of the relationships and compute
            the degrees weighted or not
            True means assume relationships weighted
        normalize : bool
            whether to normalize the values or not
            default is False, meaning values wouldn't be normalized
        preserve_parallel : bool
            do or do not count parallel relationships
            if True, if would count the parallel relationships

        Returns:
        -----------
        per_acc_date_weights : dict[float, dict[str, float]]
            the results per date degrees of each user
        """
        per_date_acc_weights: dict[float, dict[str, float]] = {}

        userIds = set(results["a_userId"].value_counts().index).union(
            results["b_userId"].value_counts().index
        )

        # a variable for normalizing
        # saving max value of each date
        date_max_values: dict[float, float] = {}

        for date in computation_date:
            per_date_acc_weights[date] = {}
            date_max_values[date] = 0
            # find the results for a specific date
            results_per_date = results[results["date"] == date]
            for user in userIds:
                relation_users = []
                results_per_date_user = results_per_date[
                    results_per_date["a_userId"] == user
                ]
                for _, row in results_per_date_user.iterrows():
                    a_userId = row["a_userId"]
                    b_userId = row["b_userId"]

                    relation = set([a_userId, b_userId])

                    # if we've counted the relation before
                    # and preserver_parallel is False
                    if relation in relation_users and not preserve_parallel:
                        continue

                    if a_userId in per_date_acc_weights[date]:
                        per_date_acc_weights[date][a_userId] += (
                            row["weight"] if weighted else 1
                        )
                    else:
                        per_date_acc_weights[date][a_userId] = (
                            row["weight"] if weighted else 1
                        )

                    # saving it not to repeat if preserve_parallel is False
                    relation_users.append(relation)

                    # updating the max value
                    if date_max_values[date] < per_date_acc_weights[date][a_userId]:
                        date_max_values[date] = per_date_acc_weights[date][a_userId]

        degree_centrality = per_date_acc_weights
        if normalize:
            degree_centrality = self.normalize_degree_centrality(
                per_date_acc_weights, date_max_values
            )

        return degree_centrality

    def normalize_degree_centrality(
        self,
        per_date_acc_weights: dict[float, dict[str, float]],
        date_max_values: dict[float, float],
    ) -> dict[float, dict[str, float]]:
        """
        normalize the per_acc_date_weights of degree centrality

        Parameters:
        ------------
        per_date_acc_weights : dict[float, dict[str, int]]
            the results per date degrees of each user
            float is representing the date and int is the weight
            str is also the user
        date_max_values : dict[float, int]
            max values in each date
            keys are dates and values are the maximum values

        Returns:
        ----------
        per_date_acc_weights : dict[float, dict[str, float]]
            the normalized version of `per_date_acc_weights`
        """
        for date in per_date_acc_weights.keys():
            for user in per_date_acc_weights[date].keys():
                # normalizing the weight
                per_date_acc_weights[date][user] = (
                    per_date_acc_weights[date][user] / date_max_values[date]
                )

        return per_date_acc_weights

    def compute_network_decentrality(
        self,
        guildId: str,
        from_start: bool,
        save: bool = True,
        weighted: bool = False,
    ) -> dict[float, float]:
        """
        compute the network decentrality over the date periods

        Parameters:
        -------------
        guildId : str
            the guildId that we want to compute the network decentraility
        save : bool
            save the results of network decentrality in db
            default is `True` meaning we would save the results back to db
        neo4j_ops : Neo4jOps
            the utils instance to save the results
            will be used if save=True, else a None value could be given
        weighted : bool
            wether to use the weights of each edge or not
            default is `False`

        Returns:
        ---------
        network_decentrality : dict[float, float]
            the decentrality over time
            keys are timestamp in float format
            values are the decenrality values
        """

        results_undirected = self.compute_degree_centerality(
            guildId=guildId,
            direction="undirected",
            weighted=weighted,
            normalize=True,
            preserve_parallel=False,
            from_start=from_start,
        )

        neo4j_metrics = Neo4JMetrics(self.neo4j_ops.gds)

        # saving each date network decentrality
        network_decentrality: dict[float, float] = {}
        for date in results_undirected.keys():
            centerality = list(results_undirected[date].values())
            network_decentrality[date] = neo4j_metrics.compute_decentralization(
                centerality
            )

        if save:
            self.save_decentralization_score(guildId, network_decentrality)

        return network_decentrality

    def save_decentralization_score(
        self,
        guildId: str,
        decentrality_score: dict[float, float],
    ) -> None:
        """
        save network decentrality scores over time in the Guild node

        Parameters:
        -------------
        guiildId : str
            the guild that we're saving data into
        decentrality_score : dict[float, float]
            the network decentrality scores over time
        """
        # preparing the queries
        queries = []
        for date in decentrality_score.keys():
            query = f"""
                MATCH (g: Guild {{guildId: '{guildId}'}})
                MERGE (g) -[r:HAVE_METRICS {{
                    date: {date}
                }}]-> (g)
                SET r.decentralizationScore = {decentrality_score[date]}
                """
            queries.append(query)

        self.neo4j_ops.store_data_neo4j(
            queries,
            message=f"GUILDID: {guildId}: Saving Network Decentrality:",
        )
