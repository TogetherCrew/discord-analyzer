import logging

import redis
from rq import Worker
from utils.credentials import get_redis_credentials


def worker_exception_handler(job, exc_type, exc_value, traceback):
    logging.error(" ========= RQ Exception =========")
    logging.error(f"JOB: {job}")
    logging.error(f"exc_type: {exc_type}")
    logging.error(f"exc_value: {exc_value}")
    logging.error(f"traceback: {traceback}")


if __name__ == "__main__":
    redis_creds = get_redis_credentials()

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    host = redis_creds["host"]
    port = redis_creds["port"]
    password = redis_creds["pass"]

    r = redis.Redis(host=host, port=port, password=password)
    worker = Worker(
        queues=["default"], connection=r, exception_handlers=worker_exception_handler
    )
    try:
        worker.work(with_scheduler=True, max_jobs=1)
    except KeyboardInterrupt:
        worker.clean_registries()
        worker.stop_scheduler()
