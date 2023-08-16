import os

from dotenv import load_dotenv
from typing import Any


def get_rabbit_mq_credentials() -> dict[str, Any]:
    """
    returns the rabbitMQ connection credentials

    Retuns:
    ----------
    rabbit_mq_creds : dict[str, Any]
        rabbitMQ credentials,
        a dictionary representive of
            `broker_url` : str
            `port` : int
            `username` : str
            `password` : str
    """
    load_dotenv()

    rabbit_mq_creds = {}

    rabbit_mq_creds["broker_url"] = os.getenv("RABBIT_HOST")
    rabbit_mq_creds["port"] = os.getenv("RABBIT_PORT")
    rabbit_mq_creds["password"] = os.getenv("RABBIT_PASSWORD")
    rabbit_mq_creds["username"] = os.getenv("RABBIT_USER")

    return rabbit_mq_creds


def get_mongo_credentials():
    """
    load mongo db credentials from .env

    Returns:
    ---------
    mongo_creds : dict[str, Any]
        mongodb credentials
        a dictionary representive of
            `user`: str
            `password` : str
            `host` : str
            `port` : int
    """
    load_dotenv()

    mongo_creds = {}

    mongo_creds["user"] = os.getenv("MONGODB_USER")
    mongo_creds["password"] = os.getenv("MONGODB_PASS")
    mongo_creds["host"] = os.getenv("MONGODB_HOST")
    mongo_creds["port"] = os.getenv("MONGODB_PORT")

    return mongo_creds


def get_neo4j_credentials():
    """
    load neo4j credentials from .env

    Returns:
    ---------
    neo4j_creds : dict[str, Any]
        neo4j credentials
        a dictionary representive of
            `user` : str
            `pass` : str
            `db_name` : str
            `url` : str
    """

    load_dotenv()

    neo4j_creds = {}
    neo4j_creds["db_name"] = os.getenv("NEO4J_DB")
    neo4j_creds["protocol"] = os.getenv("NEO4J_PROTOCOL")
    neo4j_creds["host"] = os.getenv("NEO4J_HOST")
    neo4j_creds["port"] = os.getenv("NEO4J_PORT")
    neo4j_creds["password"] = os.getenv("NEO4J_PASSWORD")
    neo4j_creds["user"] = os.getenv("NEO4J_USER")

    return neo4j_creds


def get_saga_db_location():
    """
    get the saga location in database
    """
    load_dotenv()

    saga_db = {}

    saga_db["db_name"] = os.getenv("SAGA_DB_NAME")
    saga_db["collection_name"] = os.getenv("SAGA_DB_COLLECTION")

    return saga_db


def get_sentryio_service_creds():
    load_dotenv()

    sentry_creds = {}
    sentry_creds["dsn"] = os.getenv("SENTRY_DSN")
    sentry_creds["env"] = os.getenv("SENTRY_ENV")

    return sentry_creds


def get_redis_credentials():
    """
    get redis credentials
    """
    load_dotenv()

    redis_creds = {}

    redis_creds["host"] = os.getenv("REDIS_HOST")
    redis_creds["port"] = os.getenv("REDIS_PORT")
    redis_creds["pass"] = os.getenv("REDIS_PASSWORD")

    return redis_creds
