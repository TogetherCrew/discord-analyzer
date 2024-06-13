import sentry_sdk
from utils.credentials import get_sentryio_service_creds


def set_up_sentryio(sample_rate=1.0):
    sentry_creds = get_sentryio_service_creds()
    sentry_sdk.init(
        dsn=sentry_creds["dsn"],
        environment=sentry_creds["env"],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=sample_rate,
    )
