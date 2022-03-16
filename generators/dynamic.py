import datetime
import random
import sys
import time

import pandas
from dateutil import parser

# Defaults
DELAY_INTERVAL_IN_SECONDS_DEFAULT = 5
VIEWS_TO_GENERATE_DEFAULT = 10_000
PURCHASES_TO_GENERATE_DEFAULT = 40
VIEWS_FRACTION_DEFAULT = 0.2
MIN_SECONDS_DELTA_DEFAULT = 0
MAX_SECONDS_DELTA_DEFAULT = 60
MIN_MINUTES_DELTA_DEFAULT = 0
MAX_MINUTES_DELTA_DEFAULT = 60

# Implementation

args = {}


def load_users(filepath):
    """
    Loads users from a file
    :param filepath: path to a file
    :return: pandas DataFrame with users
    """
    return pandas.read_csv(filepath, names=["user_id", "device", "ip"])


def load_items(filepath):
    """
    Loads items from a file
    :param filepath: path to a file
    :return: pandas DataFrame with items
    """
    return pandas.read_csv(filepath, names=["item_id", "item_amount", "item_price"])


def generate_random_timedelta(min_seconds_delta=MIN_SECONDS_DELTA_DEFAULT, max_seconds_delta=MAX_SECONDS_DELTA_DEFAULT,
                              min_minutes_delta=MIN_MINUTES_DELTA_DEFAULT, max_minutes_delta=MAX_MINUTES_DELTA_DEFAULT,
                              **kwargs):
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


def get_current_time_with_random_delta():
    """
    Takes current time and applies some delta on it
    :return: current time moved on some time delta
    """
    return datetime.datetime.now() + generate_random_timedelta(**args) * (1 if random.randint(0, 1) == 1 else -1)


def generate_views(users, items, view_number=VIEWS_TO_GENERATE_DEFAULT, **kwargs):
    """
    Generates table of views
    :param users: pandas DataFrame that contains all users
    :param items: pandas DataFrame that contains all items
    :param view_number: number of views to generate
    :return:
    """
    view = users.sample(view_number)
    view["item_id"] = items.sample(view_number)["item_id"].values
    view["ts"] = [get_current_time_with_random_delta() for _ in range(view_number)]
    return view


def generate_purchases(views, purchases_to_generate=PURCHASES_TO_GENERATE_DEFAULT, **kwargs):
    """
    Generates purchases based on generated views
    :param views: pandas DataFrame that contains all views
    :param purchases_to_generate: number of purchases to generate
    :return: pandas DataFrame that contains generated purchases
    """
    purchases = views.sample(purchases_to_generate)
    purchases["ts"] = [parser.parse(str(time)) + generate_random_timedelta(**args) for time in purchases["ts"].values]
    return purchases


def parse_arguments():
    """
    Parses parameter from CLI
    :return: nothing
    """
    for i in range(1, len(sys.argv), 2):
        args[sys.argv[i]] = int(sys.argv[i + 1])


def __main__():
    while True:
        parse_arguments()
        print(args)
        views = generate_views(load_users("users.csv"),
                               load_items("items.csv"), **args)
        purchases = generate_purchases(views, **args)

        print(views)
        print(purchases)

        time.sleep(DELAY_INTERVAL_IN_SECONDS_DEFAULT)


if __name__ == "__main__":
    __main__()