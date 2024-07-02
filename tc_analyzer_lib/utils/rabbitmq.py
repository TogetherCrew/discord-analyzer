import logging

from tc_analyzer_lib.utils.credentials import get_rabbit_mq_credentials
from tc_messageBroker import RabbitMQ
from tc_messageBroker.rabbit_mq.queue import Queue


class RabbitMQAccess:
    __instance = None

    def __init__(self):
        # if RabbitMQAccess.__instance is not None:
        #     raise Exception("This class is a singleton!")
        # else:
        creds = get_rabbit_mq_credentials()
        self.client = self.create_rabbitmq_client(creds)
        RabbitMQAccess.__instance = self

    @staticmethod
    def get_instance():
        # if RabbitMQAccess.__instance is None:
        try:
            RabbitMQAccess()
            logging.info("RabbitMQ broker Connected Successfully!")
        except Exception as exp:
            logging.error(f"RabbitMQ broker not connected! exp: {exp}")

        return RabbitMQAccess.__instance

    def get_client(self):
        return self.client

    def create_rabbitmq_client(self, rabbit_creds: dict[str, str]):
        rabbitmq = RabbitMQ(
            broker_url=rabbit_creds["broker_url"],
            port=rabbit_creds["port"],
            username=rabbit_creds["username"],
            password=rabbit_creds["password"],
        )
        rabbitmq.connect(queue_name=Queue.DISCORD_ANALYZER)

        return rabbitmq
