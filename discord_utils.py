import logging
from typing import Any

from analyzer_init import AnalyzerInit
from automation.automation_workflow import AutomationWorkflow
from tc_messageBroker.rabbit_mq.saga.saga_base import get_saga
from utils.get_guild_id import get_guild_id
from utils.get_rabbitmq import prepare_rabbit_mq
from utils.transactions_ordering import sort_transactions


def analyzer_recompute(sagaId: str, rabbit_creds: dict[str, Any]):
    analyzer_init = AnalyzerInit()
    analyzer, mongo_creds = analyzer_init.get_analyzer()

    saga = get_saga_instance(
        sagaId=sagaId,
        connection=mongo_creds["connection_str"],
        saga_db=mongo_creds["db_name"],
        saga_collection=mongo_creds["collection_name"],
    )
    if saga is None:
        logging.warn(
            f"Warn: Saga not found!, stopping the recompute for sagaId: {sagaId}"
        )
    else:
        platform_id = saga.data["platform._id"]
        guildId = get_guild_id(platform_id)

        def recompute_wrapper(**kwargs):
            analyzer.recompute_analytics(guildId=guildId)

        def publish_wrapper(**kwargs):
            pass

        saga.next(
            publish_method=publish_wrapper,
            call_function=recompute_wrapper,
            mongo_creds=mongo_creds,
        )

    return rabbit_creds, sagaId, mongo_creds


def analyzer_run_once(sagaId: str, rabbit_creds: dict[str, Any]):
    analyzer_init = AnalyzerInit()
    analyzer, mongo_creds = analyzer_init.get_analyzer()

    saga = get_saga_instance(
        sagaId=sagaId,
        connection=mongo_creds["connection_str"],
        saga_db=mongo_creds["db_name"],
        saga_collection=mongo_creds["collection_name"],
    )
    if saga is None:
        logging.warn(f"Saga not found!, stopping the run_once for sagaId: {sagaId}")
    else:
        platform_id = saga.data["platform._id"]
        guildId = get_guild_id(platform_id)

        def run_once_wrapper(**kwargs):
            analyzer.run_once(guildId=guildId)

        def publish_wrapper(**kwargs):
            pass

        saga.next(
            publish_method=publish_wrapper,
            call_function=run_once_wrapper,
            mongo_creds=mongo_creds,
        )
    return rabbit_creds, sagaId, mongo_creds


def get_saga_instance(sagaId: str, connection: str, saga_db: str, saga_collection: str):
    saga = get_saga(
        sagaId=sagaId,
        connection_url=connection,
        db_name=saga_db,
        collection=saga_collection,
    )
    return saga


def publish_on_success(connection, result, *args, **kwargs):
    # we must get these three things
    try:
        # rabbitmq creds
        # TODO: remove sending it in future
        _ = args[0][0]
        sagaId = args[0][1]
        mongo_creds = args[0][2]
        logging.info(f"SAGAID: {sagaId}: ON_SUCCESS callback! ")

        saga = get_saga_instance(
            sagaId=sagaId,
            connection=mongo_creds["connection_str"],
            saga_db=mongo_creds["db_name"],
            saga_collection=mongo_creds["collection_name"],
        )
        rabbitmq = prepare_rabbit_mq()

        transactions = saga.choreography.transactions

        (transactions_ordered, tx_not_started_count) = sort_transactions(transactions)

        platform_id = saga.data["platform._id"]
        guildId = get_guild_id(platform_id)

        msg = f"GUILDID: {guildId}: "
        if tx_not_started_count != 0:
            tx = transactions_ordered[0]

            logging.info(f"{msg}Publishing for {tx.queue}")

            rabbitmq.connect(tx.queue)
            rabbitmq.publish(
                queue_name=tx.queue,
                event=tx.event,
                content={"uuid": sagaId, "data": saga.data},
            )

        automation_workflow = AutomationWorkflow()
        automation_workflow.start(guild_id=guildId)

    except Exception as exp:
        logging.info(f"Exception occured in job on_success callback: {exp}")
