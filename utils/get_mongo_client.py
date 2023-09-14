from typing import Any

from pymongo import MongoClient
from utils.daolytics_uitls import get_mongo_credentials


def get_mongo_client() -> MongoClient:
    creds = get_mongo_credentials()

    connection_uri = config_mogno_creds(creds)

    client = MongoClient(connection_uri)

    return client


def config_mogno_creds(mongo_creds: dict[str, Any]):
    user = mongo_creds["user"]
    password = mongo_creds["password"]
    host = mongo_creds["host"]
    port = mongo_creds["port"]

    connection = f"mongodb://{user}:{password}@{host}:{port}"

    return connection
