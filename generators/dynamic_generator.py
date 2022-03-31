import argparse
import datetime
import logging
import random

import pandas
from dateutil import parser

from utils import FileConnector
from utils.GCPConnector import GCPConnector


def load(filepath: str, names: list) -> pandas.DataFrame:
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
    :param times:
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


def generate_oder_id(min_order_id: int,
                     max_order_id: int) -> str:
    """
    Generates random order id in range [min_order_id, max_order_id]
    :param min_order_id: minimal possible order id
    :param max_order_id: maximal possible order id
    :return: order id in range [min_order_id, max_order_id]
    """
    return f"order{random.randint(min_order_id, max_order_id)}"


def generate_orders_ids(number: int,
                        min_order_id: int,
                        max_order_id: int) -> list:
    """
    Generates array of size number and orders id in range [min_order_id, max_order_id]
    :param number: orders ids to generate
    :param min_order_id: minimal possible order id
    :param max_order_id: maximal possible order id
    :return: array of orders ids with size number, where orders id in range [min_order_id, max_order_id]
    """
    return [generate_oder_id(min_order_id, max_order_id) for _ in range(number)]


def generate_views(users: pandas.DataFrame,
                   items: pandas.DataFrame,
                   views_number: int,
                   users_fraction: float,
                   item_fraction: float,
                   min_seconds_delta: int,
                   max_seconds_delta: int,
                   min_minutes_delta: int,
                   max_minutes_delta: int) -> pandas.DataFrame:
    """
    Generates table of views
    :param item_fraction: fraction of users that will be in views and purchases generation
    :param users_fraction: fraction of items that will be in views and purchases generation
    :param users: pandas DataFrame that contains all users
    :param items: pandas DataFrame that contains all items
    :param views_number: number of views to generate
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :return: pandass DataFrame that contains views
    """
    logging.info("Started generating views")

    users_sample = users.sample(frac=users_fraction)
    items_id_sample = items["item_id"].sample(frac=item_fraction)

    joined = users_sample.join(items_id_sample, how="cross")
    views = joined.sample(views_number)

    times = []
    for _ in range(views_number):
        time = get_current_time_with_random_delta(min_seconds_delta, max_seconds_delta,
                                                  min_minutes_delta, max_minutes_delta)
        times.append(time)

    views["ts"] = times

    logging.info("Finished generating views")

    return views


def randomize_list(ls: list) -> list:
    """
    Builds new list by randomly selecting random number of elements from list
    :param ls: list to select from
    :return: list with random elements from ls and random size
    """
    new_size = random.randint(1, len(ls))
    return random.sample(ls, new_size)


def add_amounts(items: list, min_item_amount, max_item_amount) -> list:
    """
    Adds purchased amounts to items
    :param items: list of items ids
    :param min_item_amount: minimal possible item amount
    :param max_item_amount: maximal possible item amount
    :return: list of tuples (item id, amount)
    """
    result = []
    for item in items:
        amount = random.randint(min_item_amount, max_item_amount)
        result.append((item, amount))

    return result


def generate_purchases(views: pandas.DataFrame,
                       purchases_number: int,
                       min_seconds_delta: int,
                       max_seconds_delta: int,
                       min_minutes_delta: int,
                       max_minutes_delta: int,
                       min_order_id: int,
                       max_order_id: int,
                       min_item_amount: int,
                       max_item_amount: int) -> pandas.DataFrame:
    """
    Generates purchases based on generated views
    :param views: pandas DataFrame that contains all views
    :param purchases_number: number of purchases to generate
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :param min_order_id: minimal possible order id
    :param max_order_id: maximal possible order id
    :param min_item_amount: minimal possible amount of item in purchase
    :param max_item_amount: maximal possible amount of item in purchase
    :return: pandas DataFrame that contains generated purchases
    """

    logging.info("Started generating purchases")

    grouped = views.groupby(["user_id", "ip"]).agg({"ts": max, "item_id": list})

    purchases = grouped.sample(purchases_number)
    purchases["item_id"] = purchases["item_id"].apply(randomize_list)
    purchases["item_id"] = purchases["item_id"].apply(add_amounts, args=(min_item_amount, max_item_amount))

    purchases["ts"] = add_random_delta(purchases["ts"].values,
                                       min_seconds_delta, max_seconds_delta,
                                       min_minutes_delta, max_minutes_delta)

    purchases["order_id"] = generate_orders_ids(purchases_number, min_order_id, max_order_id)

    logging.info("Finished generating purchases")

    return purchases


def parse_args() -> argparse.Namespace:
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description='Dynamic generator')
    args_parser.add_argument('--views_number', default=10000, help='Views to generate', dest='views_number', type=int)
    args_parser.add_argument('--purchases_number', default=100, help='Purchases to generate', dest='purchases_number',
                             type=int)
    args_parser.add_argument('--min_seconds_delta', default=0, help='Minimal delta between seconds',
                             dest='min_seconds_delta', type=int)
    args_parser.add_argument('--max_seconds_delta', default=60, help='Maximal delta between seconds',
                             dest='max_seconds_delta', type=int)
    args_parser.add_argument('--min_minutes_delta', default=0, help='Minimal delta between minutes',
                             dest='min_minutes_delta', type=int)
    args_parser.add_argument('--max_minutes_delta', default=60, help='Maximal delta between minutes',
                             dest='max_minutes_delta', type=int)
    args_parser.add_argument('--users_fraction', default=0.3, help='Users fraction that will be used in generation',
                             dest='users_fraction', type=float)
    args_parser.add_argument('--item_fraction', default=0.3, help='Items fraction that will be used generation',
                             dest='item_fraction', type=float)
    args_parser.add_argument('--min_order_id', default=0, help='Minimal order id', dest='min_order_id', type=int)
    args_parser.add_argument('--max_order_id', default=100000, help='Maximal order id', dest='max_order_id', type=int)
    args_parser.add_argument('--sink', default='bucket', help='Data sink (possible bucket or csv)', dest='sink',
                             type=str)
    args_parser.add_argument('--bucket_name', default='capstone-project-bucket', help='Name of the bucket',
                             dest='bucket_name', type=str)
    args_parser.add_argument('--key_filepath', default='key.json', help='Key to GCP service account',
                             dest='key_filepath', type=str)
    args_parser.add_argument('--purchases_filepath', default='purchases.csv', help='Path, where to write purchases',
                             dest='purchases_filepath', type=str)
    args_parser.add_argument('--views_filepath', default='views.csv', help='Path, where to write views',
                             dest='views_filepath', type=str)
    args_parser.add_argument('--min_item_amount', default=1, help='Minimal possible amount of item in one purchase',
                             dest='min_item_amount', type=int)
    args_parser.add_argument('--max_item_amount', default=1000, help='Maximal possible amount of item in one purchase',
                             dest='max_item_amount', type=int)

    return args_parser.parse_args()


def set_up_logging() -> None:
    """
    Sets up loging output format
    """
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)


def load_resources() -> tuple:
    """
    Loads data
    :return: loaded data
    """
    logging.info("Started loading data")

    users = load("users.csv", ["user_id", "device", "ip"])
    items = load("items.csv", ["item_id", "item_amount", "item_price"])

    logging.info("Finished loading data")

    return users, items


def generate_data(args: argparse.Namespace,
                  users: pandas.DataFrame,
                  items: pandas.DataFrame) -> tuple:
    """
    Generates data
    :param args: cli arguments
    :param users: pandas DataFrame with users
    :param items: pandas DataFrame with items
    :return: generated data
    """
    logging.info("Started generating data")

    views = generate_views(users,
                           items,
                           args.views_number,
                           args.users_fraction, args.item_fraction,
                           args.min_seconds_delta, args.max_seconds_delta,
                           args.min_minutes_delta, args.max_minutes_delta)

    purchases = generate_purchases(views,
                                   args.purchases_number,
                                   args.min_seconds_delta, args.max_seconds_delta,
                                   args.min_minutes_delta, args.max_minutes_delta,
                                   args.min_order_id, args.max_order_id,
                                   args.min_item_amount, args.max_item_amount)

    logging.info("Finished generating data")

    return views, purchases


def write_data(args: argparse.Namespace,
               views: pandas.DataFrame,
               purchases: pandas.DataFrame) -> None:
    """
    Writes data to a sink
    :param args: cli arguments
    :param views: generated views
    :param purchases: generated purchases
    """
    logging.info("Started writing data")

    if args.sink.lower() == "bucket":
        GCPConnector(args.key_filepath).write_to_bucket(args.bucket_name, purchases, views)
    elif args.sink.lower() == "csv":
        FileConnector.write_DataFrame(args.views_filepath, views)
        FileConnector.write_DataFrame(args.purchases_filepath, purchases)
    else:
        raise ValueError("Sink must be bucket or csv")

    logging.info("Finished writing data")


def __main__():
    set_up_logging()

    args = parse_args()

    resources = load_resources()

    data = generate_data(args, *resources)

    write_data(args, *data)


if __name__ == "__main__":
    __main__()
