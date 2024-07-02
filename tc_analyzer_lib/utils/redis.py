import logging

import redis
from tc_analyzer_lib.utils.credentials import get_redis_credentials


class RedisSingleton:
    __instance = None

    def __init__(self):
        if RedisSingleton.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            creds = get_redis_credentials()
            self.client = self.create_redis_client(creds)
            RedisSingleton.__instance = self

    @staticmethod
    def get_instance():
        if RedisSingleton.__instance is None:
            RedisSingleton()
            try:
                info = RedisSingleton.__instance.client.ping()
                logging.info(f"Redis Connected Successfully! Ping returned: {info}")
            except Exception as exp:
                logging.error(f"Redis not connected! exp: {exp}")

        return RedisSingleton.__instance

    def get_client(self):
        return self.client

    def create_redis_client(self, redis_creds: dict[str, str]):
        return redis.Redis(
            host=redis_creds["host"],
            port=int(redis_creds["port"]),
            password=redis_creds["pass"],
            # decode_responses=True,
        )
