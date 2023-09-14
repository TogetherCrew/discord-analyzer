from tc_messageBroker import RabbitMQ
from tc_messageBroker.rabbit_mq.queue import Queue
from utils.daolytics_uitls import get_rabbit_mq_credentials


def prepare_rabbit_mq():
    """
    Prepare connection to rabbitMQ

    Returns:
    ----------
    rabbitmq : tc_messageBroker.RabbitMQ
        an instance connected to broker
    """
    rabbit_creds = get_rabbit_mq_credentials()
    rabbitmq = RabbitMQ(
        broker_url=rabbit_creds["broker_url"],
        port=rabbit_creds["port"],
        username=rabbit_creds["username"],
        password=rabbit_creds["password"],
    )
    rabbitmq.connect(queue_name=Queue.DISCORD_ANALYZER)

    return rabbitmq
