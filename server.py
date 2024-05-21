"""
start the project using rabbitMQ
"""

import functools
import logging
from typing import Any

import backoff
from discord_utils import analyzer_recompute, analyzer_run_once, publish_on_success
from pika.exceptions import AMQPConnectionError, ConnectionClosedByBroker
from redis import Redis
from rq import Queue as RQ_Queue
from utils.rabbitmq import RabbitMQSingleton
from tc_messageBroker.rabbit_mq.event import Event
from tc_messageBroker.rabbit_mq.queue import Queue
from utils.redis import RedisSingleton
from utils.sentryio_service import set_up_sentryio


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=(ConnectionClosedByBroker, ConnectionError, AMQPConnectionError),
    # waiting for 3 hours
    max_time=60 * 60 * 3,
)
def analyzer():
    # sentryio service
    set_up_sentryio()
    rabbit_mq = RabbitMQSingleton.get_instance().get_client()
    redis = RedisSingleton.get_instance().get_client()

    # 24 hours equal to 86400 seconds
    rq_queue = RQ_Queue(connection=redis, default_timeout=86400)

    analyzer_recompute = functools.partial(recompute_wrapper, redis_queue=rq_queue)
    analyzer_run_once = functools.partial(run_once_wrapper, redis_queue=rq_queue)

    rabbit_mq.connect(Queue.DISCORD_ANALYZER, heartbeat=60)

    rabbit_mq.on_event(Event.DISCORD_ANALYZER.RUN, analyzer_recompute)
    rabbit_mq.on_event(Event.DISCORD_ANALYZER.RUN_ONCE, analyzer_run_once)

    if rabbit_mq.channel is None:
        raise ConnectionError("Couldn't connect to rmq server!")
    else:
        logging.info("Started Consuming!")
        rabbit_mq.channel.start_consuming()


def recompute_wrapper(body: dict[str, Any], redis_queue: RQ_Queue):
    sagaId = body["content"]["uuid"]
    logging.info(f"SAGAID:{sagaId} recompute job Adding to queue")

    redis_queue.enqueue(
        analyzer_recompute,
        sagaId=sagaId,
        on_success=publish_on_success,
    )


def run_once_wrapper(body: dict[str, Any], redis_queue: RQ_Queue):
    sagaId = body["content"]["uuid"]
    logging.info(f"SAGAID:{sagaId} run_once job Adding to queue")
    redis_queue.enqueue(
        analyzer_run_once,
        sagaId=sagaId,
        on_success=publish_on_success,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    analyzer()
