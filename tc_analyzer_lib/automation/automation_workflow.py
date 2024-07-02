import logging
from typing import Any

from pybars import Compiler
from tc_analyzer_lib.automation.utils.automation_base import AutomationBase
from tc_analyzer_lib.automation.utils.model import AutomationDB
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue


class AutomationWorkflow(AutomationBase):
    def __init__(self) -> None:
        super().__init__()
        self.automation_db = AutomationDB()

    def start(self, platform_id: str, guild_id: str):
        """
        start the automation workflow for a guild

        Parameters
        -----------
        guild_id : str
            to select the right automation
        """
        log_prefix = f"GUILDID: {guild_id}: "
        automations = self.automation_db.load_from_db(guild_id)
        if len(automations) == 0:
            logging.info(f"{log_prefix}No automation available for this guild!")
        else:
            msg = f"{log_prefix}Starting automation!"
            logging.info(f"{msg} number of automation fetched: {len(automations)}")

        for at in automations:
            at_pre = f"{log_prefix}: automation id: {at.id}: "
            if at.enabled:
                members_by_category: dict[str, list[dict[str, str]]] = {}
                for trigger in at.triggers:
                    if trigger.enabled:
                        category: str | None = trigger.options["category"]
                        if category is None or not isinstance(category, str):
                            logging.error(
                                f"{at_pre}No category specified for one of the triggers!"
                            )
                            logging.error(f"{at_pre}Skipping the trigger!")
                            break

                        members_by_category[category] = []

                        users1, users2 = self._get_users_from_memberactivities(
                            platform_id, category
                        )
                        users = self._subtract_users(users1, users2)

                        for action in at.actions:
                            if action.enabled:
                                type = self._get_handlebar_type(action.template)
                                prepared_id_name: list[tuple[str, str]]
                                if type is None:
                                    logging.warning(
                                        f"{at_pre}No type specified in the action template!"
                                    )
                                    logging.warning(
                                        f"{at_pre}Sending raw action.template to users!"
                                    )
                                    # adding a dummy variable for user_name
                                    prepared_id_name = list(zip(users, users))
                                else:
                                    prepared_id_name = self.prepare_names(
                                        guild_id, list(users), user_field=type
                                    )

                                for user_id, user_name in prepared_id_name:
                                    compiled_message: str
                                    if type is not None:
                                        compiled_message = self._compile_message(
                                            data={type: user_name},
                                            message=action.template,
                                        )
                                    else:
                                        compiled_message = action.template

                                    data = self._prepare_saga_data(
                                        platform_id, user_id, compiled_message
                                    )
                                    saga_id = self._create_manual_saga(data)
                                    logging.info(
                                        f"{at_pre}Started to fire events for user {user_id}!"
                                    )
                                    # firing the event
                                    self.fire_event(saga_id, data)

                                    members_by_category[category].append(
                                        {"user_id": user_id, "user_name": user_name}
                                    )

                if at.report.enabled:
                    # setting up the names to send message
                    # to avoid duplicate we used dictionary
                    report_users: dict[str, str] = {}
                    for member in members_by_category[category]:  # type: ignore
                        if member["user_name"] is not None:
                            report_users[member["user_id"]] = member["user_name"]
                        else:
                            # in case of no user info was available just send the ids
                            report_users[member["user_id"]] = member["user_id"]

                    compiled_message = self._prepare_report_compiled_message(
                        list(report_users.values()), at.report.template
                    )

                    for recipent in at.report.recipientIds:
                        data = self._prepare_saga_data(
                            platform_id, recipent, compiled_message
                        )
                        saga_id = self._create_manual_saga(data)

                        # firing the event
                        self.fire_event(saga_id, data)

    def _prepare_report_compiled_message(
        self, user_names: list[str], template: str
    ) -> str:
        """
        prepare the message for the report

        Note: we're just hardcoding the template message with having the template of
        each user being in one line. and we just support the `usernames`
        """
        # hardcoding the template type for report!
        # we have to change it in future to support more types.
        type = "usernames"
        users_prepared = [f"- {user}\n" for user in user_names]

        compiled_message = self._compile_message(
            data={type: users_prepared}, message=template  # type: ignore
        )

        return compiled_message

    def _get_handlebar_type(self, template: str) -> str | None:
        """
        get the handlebar type.
        for example the template would be
        "hello {{username}}!"
        and the output would be `username`

        Note: for now it just support returning one handlebar.

        Parameters
        ------------
        template : str
            the template message to extract the type

        Returns
        ---------
        type : str
            the handlebar type to use
        """
        start_index = template.find("{{") + 2
        end_index = template.find("}}")
        if start_index == -1 or end_index == -1:
            return None
        return template[start_index:end_index]

    def _compile_message(self, data: dict[str, str], message: str) -> str:
        """
        compile the message to be sent to the user

        Parameters
        -----------
        data : dict[str, str]
            the dictionary to be compiled for the handlebars
        message : str
            the string message that contain the handlebars
        """
        compiler = Compiler()
        template = compiler.compile(message)
        compiled_message = template(data)

        return compiled_message

    def _prepare_saga_data(
        self, platform_id: str, user_id: str, message: str
    ) -> dict[str, Any]:
        """
        prepare the data needed for the saga

        Parameters:
        ------------
        platform_id : str
            the platform_id having the user
        user_id : str
            the user_id to send message
        message : str
            the message to send the user
        """
        data = {
            "platformId": platform_id,
            "created": False,
            "discordId": user_id,
            "message": message,
            "userFallback": True,
        }

        return data

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

        self.rabbitmq.publish(
            queue_name=Queue.DISCORD_BOT,
            event=Event.DISCORD_BOT.SEND_MESSAGE,
            content={
                "uuid": saga_id,
                "data": data,
            },
        )
