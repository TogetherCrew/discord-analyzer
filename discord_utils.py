import logging

from analyzer_init import AnalyzerInit
from automation.automation_workflow import AutomationWorkflow
from tc_messageBroker.rabbit_mq.saga.saga_base import get_saga
from utils.credentials import get_mongo_credentials
from utils.get_guild_utils import get_platform_guild_id, get_platform_name
from utils.rabbitmq import RabbitMQSingleton
from utils.transactions_ordering import sort_transactions


def analyzer_recompute(sagaId: str):
    saga = get_saga_instance(sagaId=sagaId)
    if saga is None:
        logging.warn(
            f"Warn: Saga not found!, stopping the recompute for sagaId: {sagaId}"
        )
    else:
        platform_id = saga.data["platformId"]
        guildId = get_platform_guild_id(platform_id)

        logging.info("Initializing the analyzer")
        analyzer_init = AnalyzerInit(guildId)
        analyzer = analyzer_init.get_analyzer()
        logging.info("Analyzer initialized")

        def recompute_wrapper(**kwargs):
            logging.info("recompute wrapper")
            analyzer.recompute_analytics()

        def publish_wrapper(**kwargs):
            pass

        logging.info("Calling the saga.next()")
        saga.next(
            publish_method=publish_wrapper,
            call_function=recompute_wrapper,
        )

    return sagaId


def analyzer_run_once(sagaId: str):
    saga = get_saga_instance(sagaId=sagaId)
    if saga is None:
        logging.warn(f"Saga not found!, stopping the run_once for sagaId: {sagaId}")
    else:
        platform_id = saga.data["platformId"]
        guildId = get_platform_guild_id(platform_id)

        analyzer_init = AnalyzerInit(guildId)
        analyzer = analyzer_init.get_analyzer()

        def run_once_wrapper(**kwargs):
            analyzer.run_once()

        def publish_wrapper(**kwargs):
            pass

        saga.next(
            publish_method=publish_wrapper,
            call_function=run_once_wrapper,
        )
    return sagaId


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


def publish_on_success(connection, result, *args, **kwargs):
    try:
        sagaId = args[0]
        logging.info(f"SAGAID: {sagaId}: ON_SUCCESS callback!")

        saga = get_saga_instance(sagaId=sagaId)
        rabbitmq = RabbitMQSingleton.get_instance().get_client()

        transactions = saga.choreography.transactions

        (transactions_ordered, tx_not_started_count) = sort_transactions(transactions)

        platform_id = saga.data["platformId"]

        msg = f"PLATFORMID: {platform_id}: "
        if tx_not_started_count != 0:
            tx = transactions_ordered[0]

            logging.info(f"{msg}Publishing for {tx.queue}")

            rabbitmq.connect(tx.queue)
            rabbitmq.publish(
                queue_name=tx.queue,
                event=tx.event,
                content={"uuid": sagaId, "data": saga.data},
            )

        guildId = get_platform_guild_id(platform_id)
        platform_name = get_platform_name(platform_id)
        # working specifically for discord
        if platform_name == "discord":
            automation_workflow = AutomationWorkflow()
            automation_workflow.start(guild_id=guildId, platform_id=platform_id)

    except Exception as exp:
        logging.info(f"Exception occured in job on_success callback: {exp}")
