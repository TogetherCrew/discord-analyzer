import logging

from automation.automation_workflow import AutomationWorkflow
from tc_messageBroker.rabbit_mq.saga.saga_base import get_saga
from utils.credentials import get_mongo_credentials
from utils.get_guild_utils import get_platform_guild_id, get_platform_name
from utils.rabbitmq import RabbitMQSingleton
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue


def get_saga_instance(sagaId: str):
    mongo_creds = get_mongo_credentials()

    saga = get_saga(
        sagaId=sagaId,
        connection_url=mongo_creds["connection_str"],
        db_name="Saga",
        collection="sagas",
    )
    if saga is None:
        raise ValueError(f"Saga with sagaId: {sagaId} not found!")

    return saga


def publish_on_success(platform_id: str, recompute: bool) -> None:
    """
    publish a message to discord platform for a specific platform
    telling the Community Manager (CM) the work is finished

    Note: this will work just for discord platform!

    Parameters
    ------------
    platform_id : str
        the discord platform to send message to
    recompute : bool
        if recompute equal to `True` then publish the job finished message for CM
        else, just run the automation
    """

    msg = f"PLATFORMID: {platform_id}: "
    logging.info(f"{msg}publishing task done to CM")

    guild_id = get_platform_guild_id(platform_id)
    platform_name = get_platform_name(platform_id)
    
    automation_workflow = AutomationWorkflow()
    # working specifically for discord
    if platform_name == "discord" and recompute:
        logging.info(f"{msg}Sending job finished message & starting automation!")
        rabbitmq = RabbitMQSingleton.get_instance().get_client()

        message = (
            "Your data import into TogetherCrew is complete! "
            "See your insights on your dashboard https://app.togethercrew.com/."
            " If you have questions send a DM to katerinabc (Discord) or k_bc0 (Telegram)."
        )
        data = {
            "platformId": platform_id,
            "created": False,
            "discordId": "user_id",  # TODO: get the CM id
            "message": message,  # the message to send
            "userFallback": True,
        }

        # creating the discord notify saga
        saga_id = automation_workflow._create_manual_saga(data=data)

        rabbitmq.publish(
            Queue.DISCORD_BOT,
            event=Event.DISCORD_BOT.SEND_MESSAGE,
            content={"uuid": saga_id},
        )
        automation_workflow.start(platform_id, guild_id)

    elif recompute is False:
        logging.info(f"{msg}Just running the automation!")
        automation_workflow.start(platform_id, guild_id)
    else:
        logging.info(
            f"{msg}platform was not discord! given platform: {platform_name}"
            "No automation or job finished message will be fired"
        )
