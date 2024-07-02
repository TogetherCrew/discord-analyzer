from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid1

from tc_analyzer_lib.utils.mongo import MongoSingleton
from tc_analyzer_lib.utils.rabbitmq import RabbitMQAccess


class AutomationBase:
    def __init__(self) -> None:
        """
        utilities for automation workflow
        """
        self.mongo_client = MongoSingleton.get_instance().get_client()
        self.rabbitmq = RabbitMQAccess.get_instance().get_client()

    def _get_users_from_guildmembers(
        self, guild_id: str, user_ids: list[str], strategy: str = "ngu"
    ) -> list[dict[str, str | None]]:
        """
        get the name of the users based on a strategy
        - `n`: nickname
        - `g`: global name
        - `u`: username

        Parameters
        -------------
        guild_id : str
            the guild_id to find users from
        user_ids : list[str]
            a list of user id to get the data
        strategy : str
            what fields of the user to select from
            can be either one of the `n`, `g`, `u` or any combination of them

        Returns
        ----------
        users_data : list[dict[str, str | None]]
            a dictionary of users with ngu names to use
        """
        user_fields = {"discordId": 1}
        for field in strategy:
            if field == "n":
                user_fields["nickname"] = 1
            elif field == "g":
                user_fields["globalName"] = 1
            elif field == "u":
                user_fields["username"] = 1
            else:
                msg = "Wrong strategy given!"
                msg += "should be either on of the `n`, `g`, or `u`!"
                raise ValueError(msg)

        user_fields["_id"] = 0

        curosr = self.mongo_client[guild_id]["guildmembers"].find(
            {"discordId": {"$in": user_ids}},
            user_fields,
        )

        users_data = list(curosr)
        return users_data

    def _get_users_from_memberactivities(
        self, db_name: str, category: str
    ) -> tuple[list[str], list[str]]:
        """
        get the users of memberactivities within a specific memberactivities
        the users from previous day and previous two days

        Parameters:
        -------------
        db_name : str
            the database to get people's id
        category : str
            the category of memberactivities

        Returns:
        ----------
        users1: list[str]
            the users for yesterday
        users2: list[str]
            the users from past two days
        """
        projection = {category: 1, "date": 1, "_id": 0}
        date_yesterday = (datetime.now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        date_two_past_days = (datetime.now() - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        users = (
            self.mongo_client[db_name]["memberactivities"]
            .find(
                {
                    "date": {
                        "$gte": date_two_past_days,
                        "$lte": date_yesterday,
                    }
                },
                projection,
            )
            .limit(2)
        )

        users1: list[str] = []
        users2: list[str] = []
        for users_data in users:
            if users_data["date"] == date_yesterday:
                users1 = users_data[category]
            else:
                users2 = users_data[category]

        return users1, users2

    def _subtract_users(self, users1: list[str], users2: list[str]) -> set[str]:
        """
        subtract two list of users

        Parameters:
        ------------
        users1: list[str]
            a list of user ids
        users2: list[str]
            a list of user ids for another day

        Returns:
        ---------
        results: set[str]
            a set of users subtracting users1 from users2
        """
        results = set(users1) - set(users2)

        return results

    def _create_manual_saga(self, data: dict[str, Any]) -> str:
        """
        manually create a saga for the discord-bot to be able to work.
        NOTE: THIS FUNCTION IS FOR MVP AND IN FUTURE WE HAVE TO ADD A NEW SAGA

        Parameters:
        ------------
        data : dict[str, Any]
            the data we want to have on the saga

        Returns:
        ---------
        saga_id : str
            the id of created saga
        """

        saga_id = str(uuid1())
        self.mongo_client["Saga"]["sagas"].insert_one(
            {
                "choreography": {
                    "name": "DISCORD_NOTIFY_USERS",
                    "transactions": [
                        {
                            "queue": "DISCORD_BOT",
                            "event": "SEND_MESSAGE",
                            "order": 1,
                            "status": "NOT_STARTED",
                        }
                    ],
                },
                "status": "IN_PROGRESS",
                "data": data,
                "sagaId": saga_id,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
            }
        )

        return saga_id

    def prepare_names(
        self, guild_id: str, user_ids: list[str], user_field: str = "username"
    ) -> list[tuple[str, str]]:
        """
        prepare the name to use in message
        just use the usernames

        Parameters
        ------------
        guild_id : str
            the guild to access their data
        user_ids : list[str]
            a list of user ids to prepare their name
        user_field : str
            the field to choose from the user
            can be either one of below
            - `username`
            - `nickname`
            - `globalName`
            - `ngu` -> is the combination of above
            - default is `username`

        Returns:
        --------
        prepared_id_name : list[tuple[str, str]]
            a prepared id and the names of users to use
            the reason we're returning the id again is we want to
            have the right alignment of id and name
        """
        # strategy selection
        fields: str
        if user_field == "ngu":
            fields = "ngu"
        else:
            fields = user_field[0]  # choose the first character

        users_data = self._get_users_from_guildmembers(
            guild_id=guild_id, user_ids=user_ids, strategy=fields
        )

        prepared_id_name: list[tuple[str, str]] = []

        if user_field == "ngu":
            for user in users_data:
                if user["nickname"] is not None:
                    prepared_id_name.append((user["discordId"], user["nickname"]))  # type: ignore
                elif user["globalName"] is not None:
                    prepared_id_name.append((user["discordId"], user["globalName"]))  # type: ignore
                else:
                    # this would never be None
                    prepared_id_name.append((user["discordId"], user["username"]))  # type: ignore
        else:
            for user in users_data:
                prepared_id_name.append((user["discordId"], user[user_field]))  # type: ignore

        return prepared_id_name
