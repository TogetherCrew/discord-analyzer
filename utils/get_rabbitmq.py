from tc_messageBroker import RabbitMQ
from tc_messageBroker.rabbit_mq.queue import Queue


def prepare_rabbit_mq(rabbit_creds):
    rabbitmq = RabbitMQ(
        broker_url=rabbit_creds["broker_url"],
        port=rabbit_creds["port"],
        username=rabbit_creds["username"],
        password=rabbit_creds["password"],
    )
    rabbitmq.connect(queue_name=Queue.DISCORD_ANALYZER)

    return rabbitmq
