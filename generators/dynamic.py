import argparse
import datetime
import random

import pandas
from dateutil import parser


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


def generate_random_timedelta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta):
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


def get_current_time_with_random_delta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta):
    """
    Takes current time and applies some delta on it
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest

    :return: current time moved on some time delta
    """
    return datetime.datetime.now() + \
           generate_random_timedelta(min_seconds_delta,
                                     max_seconds_delta,
                                     min_minutes_delta,
                                     max_minutes_delta) \
           * (1 if random.randint(0, 1) == 1 else -1)


def generate_views(users, items, views_number, users_fraction, item_fraction, min_seconds_delta, max_seconds_delta,
                   min_minutes_delta, max_minutes_delta):
    """
    Generates table of views
    :param item_fraction: fraction of users that will be in views and purchases generation
    :param users_fraction: fraction of items that will be in views and purchases generation
    :param users: pandas DataFrame that contains all users
    :param items: pandas DataFrame that contains all items
    :param views_number: number of views to generate
    :return:
    """
    view = users.sample(frac=users_fraction) \
        .join(items["item_id"].sample(frac=item_fraction), how="cross").sample(views_number)
    view["ts"] = \
        [get_current_time_with_random_delta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta)
         for _ in range(views_number)]
    return view


def generate_purchases(views, purchases_number, min_seconds_delta, max_seconds_delta, min_minutes_delta,
                       max_minutes_delta):
    """
    Generates purchases based on generated views
    :param views: pandas DataFrame that contains all views
    :param purchases_number: number of purchases to generate
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :return: pandas DataFrame that contains generated purchases
    """
    purchases = views.sample(purchases_number)
    purchases["ts"] = \
        [parser.parse(str(time)) + generate_random_timedelta(min_seconds_delta, max_seconds_delta, min_minutes_delta,
                                                             max_minutes_delta) for time in purchases["ts"].values]
    return purchases


def parse_args():
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description='Dynamic generator')
    args_parser.add_argument("--views_number", default=10_000, help="Views to generate",
                             dest="views_number")
    args_parser.add_argument("--purchases_number", default=40, help="Purchases to generate",
                             dest="purchases_number")
    args_parser.add_argument("--min_seconds_delta", default=0, help="Minimal delta between seconds",
                             dest="min_seconds_delta")
    args_parser.add_argument("--max_seconds_delta", default=60, help="Maximal delta between seconds",
                             dest="max_seconds_delta")
    args_parser.add_argument("--min_minutes_delta", default=0, help="Minimal delta between minutes",
                             dest="min_minutes_delta")
    args_parser.add_argument("--max_minutes_delta", default=60, help="Maximal delta between minutes",
                             dest="max_minutes_delta")
    args_parser.add_argument("--users_fraction", default=0.3,
                             help="Fraction of users that will be in views and purchases generation",
                             dest="users_fraction")
    args_parser.add_argument("--item_fraction", default=0.3,
                             help="Fraction of items that will be in views and purchases generation",
                             dest="item_fraction")

    return args_parser.parse_args()


def __main__():
    args = parse_args()

    views = generate_views(load_users("users.csv"), load_items("items.csv"), args.views_number, args.users_fraction,
                           args.item_fraction, args.min_seconds_delta, args.max_seconds_delta, args.min_minutes_delta,
                           args.max_minutes_delta)
    purchases = generate_purchases(views, args.purchases_number, args.min_seconds_delta, args.max_seconds_delta,
                                   args.min_minutes_delta, args.max_minutes_delta)

    views.to_csv("views.csv", index=False)
    purchases.to_csv("purchases.csv", index=False)


if __name__ == "__main__":
    __main__()
