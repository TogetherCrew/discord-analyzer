import sentry_sdk


def set_up_sentryio(dsn, environment, sample_rate=1.0):
    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=sample_rate,
    )
