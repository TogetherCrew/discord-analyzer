import logging

from rq import Worker
from utils.redis import RedisSingleton


def worker_exception_handler(job, exc_type, exc_value, traceback):
    logging.error(" ========= RQ Exception =========")
    logging.error(f"JOB: {job}")
    logging.error(f"exc_type: {exc_type}")
    logging.error(f"exc_value: {exc_value}")
    logging.error(f"traceback: {traceback}")


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    r = RedisSingleton.get_instance().get_client()
    worker = Worker(
        queues=["default"], connection=r, exception_handlers=worker_exception_handler
    )
    logging.info("Registered the worker!")
    try:
        worker.work(with_scheduler=True, max_jobs=1)
    except KeyboardInterrupt:
        worker.clean_registries()
        worker.stop_scheduler()
