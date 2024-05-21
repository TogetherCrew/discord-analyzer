from utils.credentials import (
    get_mongo_credentials,
    get_neo4j_credentials,
    get_rabbit_mq_credentials,
    get_redis_credentials,
    get_sentryio_service_creds,
)


def test_mongo_creds_keys():
    """
    test whether the keys of dictionaries is created or not
    """
    mongo_creds = get_mongo_credentials()

    credential_keys = list(mongo_creds.keys())

    assert "user" in credential_keys
    assert "password" in credential_keys
    assert "host" in credential_keys
    assert "port" in credential_keys


def test_mongo_creds_values():
    mongo_creds = get_mongo_credentials()

    assert mongo_creds["user"] is not None
    assert mongo_creds["password"] is not None
    assert mongo_creds["host"] is not None
    assert mongo_creds["port"] is not None


def test_rabbit_creds_keys():
    rabbit_creds = get_rabbit_mq_credentials()

    credential_keys = list(rabbit_creds.keys())

    assert "broker_url" in credential_keys
    assert "port" in credential_keys
    assert "password" in credential_keys
    assert "username" in credential_keys


def test_rabbit_creds_values():
    rabbit_creds = get_rabbit_mq_credentials()

    assert rabbit_creds["broker_url"] is not None
    assert rabbit_creds["port"] is not None
    assert rabbit_creds["password"] is not None
    assert rabbit_creds["username"] is not None


def test_no4j_creds_keys():
    neo4j_creds = get_neo4j_credentials()

    credential_keys = list(neo4j_creds.keys())

    assert "user" in credential_keys
    assert "password" in credential_keys
    assert "db_name" in credential_keys
    assert "protocol" in credential_keys
    assert "port" in credential_keys
    assert "host" in credential_keys


def test_neo4j_creds_values():
    neo4j_creds = get_neo4j_credentials()

    assert neo4j_creds["user"] is not None
    assert neo4j_creds["password"] is not None
    assert neo4j_creds["protocol"] is not None
    assert neo4j_creds["port"] is not None
    assert neo4j_creds["db_name"] is not None
    assert neo4j_creds["host"] is not None


def test_redis_creds_keys():
    redis_creds = get_redis_credentials()

    credential_keys = list(redis_creds.keys())

    assert "pass" in credential_keys
    assert "port" in credential_keys
    assert "host" in credential_keys


def test_redis_creds_values():
    redis_creds = get_redis_credentials()

    assert redis_creds["pass"] is not None
    assert redis_creds["port"] is not None
    assert redis_creds["host"] is not None

def test_sentryio_creds():
    sentry_creds = get_sentryio_service_creds()

    assert "dsn" in sentry_creds
    assert "env" in sentry_creds


def test_sentryio_creds_values():
    sentry_creds = get_sentryio_service_creds()

    assert sentry_creds["dsn"] is not None
    assert sentry_creds["env"] is not None
