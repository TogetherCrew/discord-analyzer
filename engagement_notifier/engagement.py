import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid1

from engagement_notifier.messages import disengaeged_message
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue
from utils.get_mongo_client import get_mongo_client
from utils.get_rabbitmq import prepare_rabbit_mq


class EngagementNotifier:
    def __init__(self) -> None:
        self.mongo_client = get_mongo_client()
        self.rabbitmq = prepare_rabbit_mq()

    def notify_disengaged(
        self, guild_id: str, category: str = "all_new_disengaged"
    ) -> None:
        """
        notify the disengaged type of people

        Parameters:
        ------------
        guild_id : str
            the guild id to notify people
        category : str
            which category of disengaged memberactivities to notify
        """
        users1, users2 = self._get_users(guild_id, category)
        users = self._subtract_users(users1, users2)

        msg = f"GUILDID: {guild_id}: "

        for user_id in users:
            logging.info(f"{msg}Firing event for user: {user_id}")
            # creating the saga in database
            data = self._prepare_saga_data(guild_id, user_id)
            saga_id = self._create_manual_saga(data)
            # firing the event
            self.fire_event(saga_id, data)

    def fire_event(self, saga_id: str, data: dict[str, Any]) -> None:
        """
        fire the event `SEND_MESSAGE` to the user of a guild

        Parameters:
        ------------
        saga_id : str
            the saga_id having of the event
        data : str
            the data to fire
        """

        self.rabbitmq.connect(Queue.DISCORD_BOT)

        self.rabbitmq.publish(
            queue_name=Queue.DISCORD_BOT,
            event=Event.DISCORD_BOT.SEND_MESSAGE,
            content={
                "uuid": saga_id,
                "data": data,
            },
        )

    def _prepare_saga_data(self, guild_id: str, user_id: str) -> dict[str, Any]:
        """
        prepare the data needed for the saga

        Parameters:
        ------------
        guild_id : str
            the guild_id having the user
        user_id : str
            the user_id to send message
        """
        data = {
            "guildId": guild_id,
            "created": False,
            "discordId": user_id,
            "message": disengaeged_message,
            "userFallback": True,
        }

        return data

    def _get_users(self, guild_id: str, category: str) -> tuple[list[str], list[str]]:
        """
        get the users of memberactivities within a specific memberactivities
        the users from previous day and previous two days

        Parameters:
        -------------
        guild_id : str
            the guild id to get people's id
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
        date_yesterday = (
            (datetime.now() - timedelta(days=1))
            .replace(hour=0, minute=0, second=0)
            .strftime("%Y-%m-%dT%H:%M:%S")
        )

        date_two_past_days = (
            (datetime.now() - timedelta(days=2))
            .replace(hour=0, minute=0, second=0)
            .strftime("%Y-%m-%dT%H:%M:%S")
        )

        users = (
            self.mongo_client[guild_id]["memberactivities"]
            .find(
                {
                    "$or": [
                        {"date": date_yesterday},
                        {"date": date_two_past_days},
                    ]
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
