import logging
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid1

from pymongo import MongoClient
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue
from utils.daolytics_uitls import get_mongo_credentials, get_rabbit_mq_credentials
from utils.get_rabbitmq import prepare_rabbit_mq


class EngagementNotifier:
    def __init__(self) -> None:
        pass

    def notify_disengaged(
        self, guildId: str, category: str = "all_new_disengaged"
    ) -> None:
        """
        notify the disengaged type of people

        Parameters:
        ------------
        guildId : str
            the guild id to notify people
        category : str
            which category of disengaged memberactivities to notify
        """
        users1, users2 = self._get_users(guildId, category)
        users = self._subtract_users(users1, users2)

        msg = f"GUILDID: {guildId}: "
        # hardcoding a guildId for now
        if guildId == "915914985140531240":
            for userId in users:
                logging.info(f"{msg}Firing event for user: {userId}")
                self.fire_event(guildId, userId)
        else:
            logging.warning(f"{msg}This guild is not included for notifier!")

    def fire_event(self, guildId: str, userId: str) -> None:
        """
        fire the event `SEND_MESSAGE` to the user of a guild

        Parameters:
        ------------
        guildId : str
            the guildId having the user
        userId : str
            the userId to send message
        """
        rabbit_creds = get_rabbit_mq_credentials()
        rabbitmq = prepare_rabbit_mq(rabbit_creds)

        rabbitmq.connect(Queue.DISCORD_BOT)

        rabbitmq.publish(
            queue_name=Queue.DISCORD_BOT,
            event=Event.DISCORD_BOT.SEND_MESSAGE,
            content={
                "uuid": str(uuid1()),
                "data": {
                    "guildId": guildId,
                    "created": False,
                    "discordId": userId,
                    "message": "This message is sent you for notifications!",
                    "userFallback": True,
                },
            },
        )

    def _get_users(self, guildId: str, category: str) -> tuple[list[str], list[str]]:
        """
        get the users of memberactivities within a specific memberactivities
        the users from previous day and previous two days

        Parameters:
        -------------
        guildId : str
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

        client = self.setup_client()
        users = (
            client[guildId]["memberactivities"]
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

    def setup_client(self) -> MongoClient:
        creds = get_mongo_credentials()

        connection_uri = self._get_mongo_connection(creds)

        client = MongoClient(connection_uri)

        return client

    def _get_mongo_connection(self, mongo_creds: dict[str, Any]):
        user = mongo_creds["user"]
        password = mongo_creds["password"]
        host = mongo_creds["host"]
        port = mongo_creds["port"]

        connection = f"mongodb://{user}:{password}@{host}:{port}"

        return connection
