from time import time

MINUTE_IN_SECONDS = 60
HOUR_IN_SECONDS = MINUTE_IN_SECONDS * 60
WEEK_IN_SECONDS = HOUR_IN_SECONDS * 24 * 7
YEAR_IN_SECONDS = HOUR_IN_SECONDS * 24 * 365


def get_time_1h_ago(time=time()):
    return int(time) - HOUR_IN_SECONDS


def get_time_1w_later(time=time()):
    return int(time) + WEEK_IN_SECONDS


def get_time_1w_ago(time=time()):
    return int(time) - WEEK_IN_SECONDS


def get_timestamps(time=time()):
    def to_str(x):
        return str(x)

    return dict(
        timestamp_gte=to_str(time), timestamp_lt=to_str(time + (MINUTE_IN_SECONDS * 10))
    )


def get_time_24h_ago(time=time()):
    return int(time - (HOUR_IN_SECONDS * 24))


def get_time_1w_ago(time=time()):
    return int(time - WEEK_IN_SECONDS)
