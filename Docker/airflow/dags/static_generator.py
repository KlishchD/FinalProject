import argparse
import logging
from random import randint, uniform

import FileConnector
import RedisConnector

USED_ITEM_ID = {-1}

USED_USER_ID = {-1}
USED_IP_ADDRESSES = {"0.0.0.0"}

cli_arguments_config = [
    ["--items_number", 10_000, "Items to generate", "items_number", int],
    ["--min_item_id", 0, "Minimal item id", "min_item_id", int],
    ["--max_item_id", 100_000_000, "Maximal item id", "max_item_id", int],
    ["--min_item_price", 0.5, "Minimal item price", "min_item_price", float],
    ["--max_item_price", 100_000, "Maximal item price", "max_item_price", float],
    ["--users_number", 10_000, "Users to generate", "users_number", int],
    ["--min_user_id", 1, "Minimal user id", "min_user_id", int],
    ["--max_user_id", 10_000_000, "Maximal user id", "max_user_id", int],
    ["--min_devices_number", 1, "Minimal devices number", "min_devices_number", int],
    ["--max_devices_number", 5, "Maximal devices number", "max_devices_number", int],
    ["--sink", "redis", "Data sink (possible redis or csv)", "sink", str],
    ["--redis_port", "6060", "Port on which redis runs", "redis_port", int],
    ["--redis_host", "redis", "Host on which redis runs", "redis_host", str],
    ["--users_filepath", "users.csv", "Path were to write users", "users_filepath", str],
    ["--items_filepath", "items.csv", "Path were to write items", "items_filepath", str],
    ["--ips_filepath", "ips.csv", "Path were to write ips", "ips_filepath", str],
    ["--countries_filepath", "resources/countries.txt", "Path to a file with possible countries", "countries_filepath",
     str],
    ["--devices_filepath", "resources/devices.txt", "Path to a file with possible devices", "devices_filepath", str]
]


def load_countries(filepath: str) -> None:
    """
    Loads countries from the file
    :param filepath: path to countries file
    :return: array with loaded countries
    """
    logging.info('Started loading possible countries')

    global COUNTRIES
    COUNTRIES = [line.strip() for line in open(filepath)]

    logging.info('Finished loading possible countries')


def load_devices(filepath: str) -> None:
    """
    Loads devices from the file
    :param filepath: path to devices file
    :return: array with loaded devices
   """
    logging.info('Started loading possible devices')

    global DEVICES
    DEVICES = [line.strip() for line in open(filepath)]

    logging.info('Finished loading possible devices')


def load(devices_filepath: str,
         countries_filepath: str) -> None:
    logging.info("Started loading data")
    load_devices(devices_filepath)
    load_countries(countries_filepath)
    logging.info("Finished loading data")


def generate_users(users_number: int,
                   min_user_id: int,
                   max_user_id: int,
                   min_devices_number: int,
                   max_devices_number: int) -> list:
    """
    Generates users
    :param users_number: number of users to generate
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: list of users
    """
    logging.info("Started generating users")
    users = [generate_user(min_user_id, max_user_id, min_devices_number, max_devices_number) for _ in
             range(users_number)]
    logging.info("Finished generating users")

    return users


def generate_user(min_user_id: int,
                  max_user_id: int,
                  min_devices_number: int,
                  max_devices_number: int) -> tuple:
    """
    Generates user
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: tuple that represents user
    """
    id = generate_user_id(min_user_id, max_user_id)
    devices = generate_devices(min_devices_number, max_devices_number)
    country = generate_country()
    return id, devices, country


def generate_user_id(min_user_id: int,
                     max_user_id: int) -> str:
    """
    Generates user id
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: unique user id
    """
    user_id = -1
    while user_id in USED_USER_ID:
        user_id = randint(min_user_id, max_user_id)
    USED_USER_ID.add(user_id)
    return f"user{str(user_id)}"


def generate_devices(min_devices_number: int,
                     max_devices_number: int) -> list:
    """
    Generates devices
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :return: list of tuples that represent devices
    """
    devices = []
    for _ in range(randint(min_devices_number, max_devices_number)):
        devices.append((DEVICES[randint(0, len(DEVICES) - 1)], generate_random_ip()))

    return devices


def generate_random_ip() -> str:
    """
    Generates random ip that hasn't been generated by it before
    :return: random ip that hasn't been generated by this function before
    """
    ip = "0.0.0.0"
    while ip in USED_IP_ADDRESSES:
        ip = ".".join([str(randint(0, 255)) for _ in range(4)])

    USED_IP_ADDRESSES.add(ip)

    return ip


def generate_country() -> str:
    """
    Randomly peeks a country from a list of possible
    :return: country
    """
    return COUNTRIES[randint(0, len(COUNTRIES) - 1)]


def extract_ips_from_users(users: list) -> list:
    """
    Extracts ips from list of users
    :param users: list of users
    :return: list of ips
    """
    logging.info("Started extracting ips from users")
    ips = []
    for user in users:
        for device in user[1]:
            ips.append((device[1], user[2]))
    logging.info("Finished extracting ips from users")
    return ips


def generate_items(items_number: int,
                   min_item_id: int,
                   max_item_id: int,
                   min_item_price: float,
                   max_item_price: float) -> list:
    """
    Generates items
    :param items_number: number of items to generate
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: list of items
    """
    logging.info("Started generating items")
    items = [generate_item(min_item_id, max_item_id, min_item_price, max_item_price) for _ in range(items_number)]
    logging.info("Finished generating items files")
    return items


def generate_item(min_item_id: int,
                  max_item_id: int,
                  min_item_price: float,
                  max_item_price: float) -> tuple:
    """
    Generates item
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: tuple that represents item
    """
    return generate_item_id(min_item_id, max_item_id), generate_item_price(min_item_price, max_item_price)


def generate_item_id(min_item_id: int,
                     max_item_id: int) -> str:
    """
    Generates unique item id
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :return: unique item id
    """
    item_id = -1
    while item_id in USED_ITEM_ID:
        item_id = randint(min_item_id, max_item_id)

    USED_ITEM_ID.add(item_id)

    return f"item{str(item_id)}"


def generate_item_price(min_item_price: float,
                        max_item_price: float) -> str:
    """
    Generates price
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: price
    """
    return str(uniform(min_item_price, max_item_price))


def parse_arguments() -> argparse.Namespace:
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description="Static generator")

    for argument in cli_arguments_config:
        args_parser.add_argument(argument[0], default=argument[1], help=argument[2], dest=argument[3], type=argument[4])

    return args_parser.parse_args()


def set_up_logging() -> None:
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)


def generate_data(args: argparse.Namespace) -> tuple:
    users = generate_users(args.users_number,
                           args.min_user_id,
                           args.max_user_id,
                           args.min_devices_number,
                           args.max_devices_number)

    ips = extract_ips_from_users(users)

    items = generate_items(args.items_number,
                           args.min_item_id,
                           args.max_item_id,
                           args.min_item_price,
                           args.max_item_price)
    return users, ips, items


def write_data(args: argparse.Namespace,
               users: list,
               ips: list,
               items: list) -> None:
    logging.info("Started writing data")

    if args.sink.lower() == "redis":
        redis_connector = RedisConnector.RedisConnector(args.redis_host, args.redis_port)
        redis_connector.write(users, ips, items)
    elif args.sink.lower() == "csv":
        FileConnector.write_users(args.users_filepath, users)
        FileConnector.write_ips(args.ips_filepath, ips)
        FileConnector.write_items(args.items_filepath, items)
    else:
        raise ValueError("Sink must be csv or redis")

    logging.info("Finished writing data")


def __main__():
    set_up_logging()
    args = parse_arguments()
    load(args.devices_filepath, args.countries_filepath)
    write_data(args, *generate_data(args))


if __name__ == "__main__":
    __main__()
