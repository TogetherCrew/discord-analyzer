# analyzer whether a node is sender or receiver
import logging
from uuid import uuid1

import pandas as pd
from discord_analyzer.analysis.neo4j_utils.projection_utils import ProjectionUtils
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class NodeStats:
    def __init__(self, threshold: int = 2) -> None:
        """
        initialize the Node status computations object
        the status could be either one of `Sender`, `Receiver`, `Balanced`

        Parameters:
        -------------
        gds : GraphDataScience
            the gds instance to do computations on it
        neo4j_ops : Neo4jOps
            neo4j shared library instance to use
        threshold : int
            the threshold value to compute the stats
            default is 2 meaning for the node
            - If in_degrees > threhold * out_degree then it's frequent receive
            - else if out_degrees > threhold * in_degree then it's frequent sender
            - else it is balanced

        """
        neo4j_ops = Neo4jOps.get_instance()
        self.gds = neo4j_ops.gds
        self.driver = neo4j_ops.neo4j_driver
        self.threshold = threshold

    def compute_stats(self, guildId: str, from_start: bool) -> None:
        projection_utils = ProjectionUtils(guildId=guildId)

        # possible dates to do the computations
        possible_dates = projection_utils.get_dates(guildId=guildId)

        # if we didn't want to compute from the day start
        if not from_start:
            computed_dates = self.get_computed_dates(projection_utils, guildId)
            possible_dates = possible_dates - computed_dates

        for date in possible_dates:
            try:
                self.compute_node_stats_wrapper(projection_utils, guildId, date)
            except Exception as exp:
                msg = f"GUILDID: {guildId} "
                logging.error(
                    f"{msg} node stats computation for date: {date}, exp: {exp}"
                )

    def compute_node_stats_wrapper(
        self, projection_utils: ProjectionUtils, guildId: str, date: float
    ):
        """
        a wrapper for node stats computation process
        we're doing the projection here and computing on that,
        then we'll drop the pojection

        Parameters:
        ------------
        projection_utils : ProjectionUtils
            the utils needed to get the work done
        guildId : str
            the guild we want the temp relationships
            between its members
        date : float
            timestamp of the relation
        """
        # NATURAL relations direction degreeCentrality computations
        graph_name = f"GraphStats_{uuid1()}"

        projection_utils.project_temp_graph(
            guildId=guildId,
            graph_name=graph_name,
            weighted=True,
            relation_direction="NATURAL",
            date=date,
        )
        natural_dc = self.gds.run_cypher(
            """
            CALL gds.degree.stream(
                $graph_name,
                {
                    relationshipWeightProperty: 'weight'
                }
            )
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).userId AS userId, score
            """,
            {
                "graph_name": graph_name,
            },
        )

        reverse_dc = self.gds.run_cypher(
            """
            CALL gds.degree.stream(
                $graph_name,
                {
                    orientation: 'REVERSE',
                    relationshipWeightProperty: 'weight'
                }
            )
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).userId AS userId, score
            """,
            {
                "graph_name": graph_name,
            },
        )

        df = self.get_date_stats(natural_dc, reverse_dc, threshold=self.threshold)

        self.save_properties_db(guildId, df, date)
        _ = self.gds.run_cypher(
            "CALL gds.graph.drop($graph_name)",
            {
                "graph_name": graph_name,
            },
        )

    def get_computed_dates(
        self, projection_utils: ProjectionUtils, guildId: str
    ) -> set[float]:
        """
        get the computed dates of our guild
        """
        query = """
            MATCH (:DiscordAccount)
                -[r:INTERACTED_IN]->(g:Guild {guildId: $guild_id})
            WHERE r.status IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = projection_utils.get_computed_dates(
            query=query, guild_id=guildId
        )

        return computed_dates

    def get_date_stats(
        self, sender_info: pd.DataFrame, reciever_info: pd.DataFrame, threshold: int
    ) -> pd.DataFrame:
        merged_df = pd.merge(
            sender_info, reciever_info, on="userId", suffixes=("_S", "_R")
        )
        # getting the ones that at least receiver or sender count
        merged_df = merged_df[(merged_df["score_R"] != 0) | (merged_df["score_S"] != 0)]

        # Frequent Receiver
        merged_df["freq_reciver"] = (
            merged_df["score_R"] > threshold * merged_df["score_S"]
        )

        # Frequent Sender
        merged_df["freq_sender"] = (
            merged_df["score_S"] > threshold * merged_df["score_R"]
        )

        merged_df = self._compute_stats(merged_df)

        del merged_df["freq_reciver"]
        del merged_df["freq_sender"]
        del merged_df["score_R"]
        del merged_df["score_S"]

        return merged_df

    def _compute_stats(
        self,
        merged_df: pd.DataFrame,
        sender_col: str = "freq_sender",
        receiver_col: str = "freq_reciver",
    ) -> pd.DataFrame:
        """
        get the final conclusion of user stats
        the user must be either Receiver, Sender, or Balanceed
        saving back to a column named `Balanced`

        Parameters:
        ------------
        merged_df : pd.Dataframe
            the dataframe that merged the degreeCentralities column
        sender_col : str
            column named representing the question of "is the user Sender?"
            default is "freq_sender"
        receiver_col : str
            column named representing the question of "is the user Receiver?"
            default is "freq_reciver"

        Returns:
        ---------
        merged_df : pd.DataFrame
            returning the dataframe with a column named `stats`
        """

        stats = []
        for _, row in merged_df.iterrows():
            sender = row[sender_col]
            receiver = row[receiver_col]

            if sender:
                stats.append(0)
            elif receiver:
                stats.append(1)
            elif not sender and not receiver:
                stats.append(2)
            else:
                # S-> Sender
                # R -> Receiver
                logging.error("It isn't possible to have both S and R True!")

        merged_df["stats"] = stats

        return merged_df

    def save_properties_db(
        self, guildId: str, user_status: pd.DataFrame, date: float
    ) -> None:
        """
        save user stats to their nodes

        Parameters:
        ------------
        guildId : str
            the guildId we're using
        user_status : pd.DataFrame
            dataframe containing `userId` and `stats` for each user
        date : float
            the date in timestamp format
        """
        with self.driver.session() as session:
            for _, row in user_status.iterrows():
                userId = row["userId"]
                status = row["stats"]

                query = """
                    MATCH (a:DiscordAccount {userId: $userId})
                    MATCH (g:Guild {guildId: $guildId})
                    MERGE (a) -[r:INTERACTED_IN {
                        date: $date
                    }] -> (g)
                    SET r.status = $status
                """
                session.run(
                    query, userId=userId, guildId=guildId, status=status, date=date
                )
        prefix = f"GUILDID: {guildId}: "
        logging.info(f"{prefix}Node stats saved for the date: {date}")
