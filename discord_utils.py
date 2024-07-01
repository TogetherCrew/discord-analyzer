import logging

from automation.automation_workflow import AutomationWorkflow
from tc_messageBroker.rabbit_mq.saga.saga_base import get_saga
from utils.credentials import get_mongo_credentials
from utils.get_guild_utils import get_platform_guild_id, get_platform_name
from utils.rabbitmq import RabbitMQSingleton


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


def publish_on_success(platform_id: str):
    """
    publish a message to discord platform for a specific platform
    telling the Community Manager the work is finished

    Note: this shuold always send message in case of recompute equal to `True`

    Parameters
    ------------
    platform_id : str
        the discord platform to send message to
    """

    msg = f"PLATFORMID: {platform_id}: "
    logging.info(f"{msg}publishing task done to CM")

    guildId = get_platform_guild_id(platform_id)
    platform_name = get_platform_name(platform_id)
    # working specifically for discord
    if platform_name == "discord":
        rabbitmq = RabbitMQSingleton.get_instance().get_client()
        automation_workflow = AutomationWorkflow()

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

        automation_workflow._create_manual_saga()
        
        automation_workflow.start(guild_id=guildId, platform_id=platform_id)
