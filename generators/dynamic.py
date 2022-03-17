import argparse
import datetime
import logging
import random

import pandas
from dateutil import parser


def load(filepath, names):
    """
    Loads users from a file
    :param filepath: path to a file
    :param names: list that contains column names
    :return: pandas DataFrame with users
    """
    logging.info(f"Started loading {filepath}")
    data = pandas.read_csv(filepath, names=names)
    logging.info(f"Finished loading {filepath}")
    return data


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
    Generates random time from current time by applying random time delta on it
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


def add_random_delta(times, min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta):
    """
    Generates array with times from original one, but moved on some random time delta
    :param times:
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest

    :return: current time moved on some time delta
    """
    return [parser.parse(str(time)) + generate_random_timedelta(min_seconds_delta, max_seconds_delta, min_minutes_delta,
                                                                max_minutes_delta) for time in times]


def generate_views(users, items, views_number, users_fraction, item_fraction, min_seconds_delta, max_seconds_delta,
                   min_minutes_delta, max_minutes_delta):
    """
    Generates table of views
    :param item_fraction: fraction of users that will be in views and purchases generation
    :param users_fraction: fraction of items that will be in views and purchases generation
    :param users: pandas DataFrame that contains all users
    :param items: pandas DataFrame that contains all items
    :param views_number: number of views to generate
    :return: pandass DataFrame that contains views
    """
    logging.info("Started generating views")

    users_sample = users.sample(frac=users_fraction)
    items_id_sample = items["item_id"].sample(frac=item_fraction)

    joined = users_sample.join(items_id_sample, how="cross")
    views = joined.sample(views_number)

    views["ts"] = \
        [get_current_time_with_random_delta(min_seconds_delta, max_seconds_delta, min_minutes_delta, max_minutes_delta)
         for _ in range(views_number)]

    logging.info("Finished generating views")

    return views


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

    logging.info("Started generating purchases")

    purchases = views.sample(purchases_number)
    purchases["ts"] = add_random_delta(purchases["ts"].values, min_seconds_delta, max_seconds_delta,
                                       min_minutes_delta, max_minutes_delta)

    logging.info("Finished generating purchases")

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


def set_up_logging():
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)


def write_to_file(filepath, data, header=True):
    """
    Writes data from pandas DataaFrame to file
    :param header: flag says whether to write header or not
    :param filepath: path where to write a file
    :param data: pandas DataFrame to write
    :return: nothing
    """
    logging.info(f"Started writing {filepath}")
    data.to_csv(filepath, index=False, header=header)
    logging.info(f"Finished writing {filepath}")


def __main__():
    set_up_logging()

    args = parse_args()

    users = load("users.csv", ["user_id", "device", "ip"])
    items = load("items.csv", ["item_id", "item_amount", "item_price"])

    views = generate_views(users, items, args.views_number, args.users_fraction,
                           args.item_fraction, args.min_seconds_delta, args.max_seconds_delta, args.min_minutes_delta,
                           args.max_minutes_delta)

    purchases = generate_purchases(views, args.purchases_number, args.min_seconds_delta, args.max_seconds_delta,
                                   args.min_minutes_delta, args.max_minutes_delta)

    write_to_file("views.csv", views)
    write_to_file("purchases.csv", purchases)


if __name__ == "__main__":
    __main__()
