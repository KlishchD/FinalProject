import datetime
from dateutil import parser
import random

def generate_random_timedelta(min_seconds_delta: int,
                              max_seconds_delta: int,
                              min_minutes_delta: int,
                              max_minutes_delta: int) -> datetime.timedelta:
    """
    Generates random time delta
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :return: time delta
    """
    return datetime.timedelta(minutes=random.randint(min_minutes_delta, max_minutes_delta),
                              seconds=random.randint(min_seconds_delta, max_seconds_delta))


def get_current_time_with_random_delta(min_seconds_delta: int,
                                       max_seconds_delta: int,
                                       min_minutes_delta: int,
                                       max_minutes_delta: int) -> datetime.datetime:
    """
    Generates random time from current time by applying random time delta on it
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :return: current time moved on some time delta
    """
    time = datetime.datetime.now()
    delta = generate_random_timedelta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta)
    sign = 1 if random.randint(0, 1) == 1 else -1
    return time + delta * sign


def add_random_delta(times: list,
                     min_seconds_delta: int,
                     max_seconds_delta: int,
                     min_minutes_delta: int,
                     max_minutes_delta: int) -> list:
    """
    Generates array with times from original one, but moved on some random time delta
    :param times: list with times
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest

    :return: current time moved on some time delta
    """
    result = []
    for time in times:
        time = parser.parse(str(time))
        delta = generate_random_timedelta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta)
        result.append(time + delta)

    return result
