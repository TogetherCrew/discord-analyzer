from tc_analyzer_lib.utils.credentials import get_rabbit_mq_credentials
from tc_messageBroker.message_broker import RabbitMQ


def test_rabbit_mq_connect():
    rabbit_creds = get_rabbit_mq_credentials()
    rabbit_mq = RabbitMQ(
        broker_url=rabbit_creds["broker_url"],
        port=rabbit_creds["port"],
        username=rabbit_creds["username"],
        password=rabbit_creds["password"],
    )

    connect = rabbit_mq.connect("sample_queue")

    # when no rabbitmq instance is running it should be False
    assert connect is not False
