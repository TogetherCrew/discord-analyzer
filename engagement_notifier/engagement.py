import logging
from typing import Any

from engagement_notifier.messages import get_disengaged_message
from engagement_notifier.utils import EngagementUtils
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue


class EngagementNotifier(EngagementUtils):
    def __init__(self) -> None:
        super().__init__()

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
        users1, users2 = self._get_users_from_memberactivities(guild_id, category)
        users = self._subtract_users(users1, users2)
        names = self.prepare_names(guild_id, list(users))

        msg = f"GUILDID: {guild_id}: "

        for user_id, user_name in zip(users, names):
            logging.info(f"{msg}Firing event for user: {user_id}")

            # preparations
            message = get_disengaged_message(user_name)
            data = self._prepare_saga_data(guild_id, user_id, message)
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

    def _prepare_saga_data(
        self, guild_id: str, user_id: str, message: str
    ) -> dict[str, Any]:
        """
        prepare the data needed for the saga

        Parameters:
        ------------
        guild_id : str
            the guild_id having the user
        user_id : str
            the user_id to send message
        message : str
            the message to send the user
        """
        data = {
            "guildId": guild_id,
            "created": False,
            "discordId": user_id,
            "message": message,
            "userFallback": True,
        }

        return data

    def prepare_names(self, guild_id: str, user_ids: list[str]) -> list[str]:
        """
        prepare the name to use in message
        selecting from ngu meaning we would choose the name in first item
        and if was None, we would choose the next ones
        1. NickName
        2. GlobalName
        3. Username

        Parameters
        ------------
        guild_id : str
            the guild to access their data
        user_ids : list[str]
            a list of user ids to prepare their name


        Returns:
        --------
        prepared_names : list[str]
            a prepared names of users to use
        """
        users_data = self._get_users_from_guildmembers(
            guild_id=guild_id, user_ids=user_ids
        )

        prepared_names: list[str] = []

        for user in users_data:
            if user["nickname"] is not None:
                prepared_names.append(user["nickname"])
            elif user["globalName"] is not None:
                prepared_names.append(user["globalName"])
            else:
                # this would never be None
                prepared_names.append(user["username"])  # type: ignore

        return prepared_names
