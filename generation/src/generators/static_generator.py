import argparse
import logging
from random import randint, uniform

from Utils import RedisConnector, FileConnector

USED_ITEM_ID = {-1}

USED_USER_ID = {-1}
USED_IP_ADDRESSES = {"0.0.0.0"}


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
    """
    Loads resources
    :param devices_filepath: path where file with possible devices reside
    :param countries_filepath: path where file with possible countries reside
    """
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
    return str(round(uniform(min_item_price, max_item_price), 2))


def parse_arguments() -> argparse.Namespace:
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description="Static generator")

    args_parser.add_argument('--items_number', default=10000, help='Items to generate', dest='items_number', type=int)
    args_parser.add_argument('--min_item_id', default=0, help='Minimal item id', dest='min_item_id', type=int)
    args_parser.add_argument('--max_item_id', default=100000000, help='Maximal item id', dest='max_item_id', type=int)
    args_parser.add_argument('--min_item_price', default=0.5, help='Minimal item price', dest='min_item_price',
                             type=float)
    args_parser.add_argument('--max_item_price', default=100000, help='Maximal item price', dest='max_item_price',
                             type=float)
    args_parser.add_argument('--users_number', default=10000, help='Users to generate', dest='users_number', type=int)
    args_parser.add_argument('--min_user_id', default=1, help='Minimal user id', dest='min_user_id', type=int)
    args_parser.add_argument('--max_user_id', default=10000000, help='Maximal user id', dest='max_user_id', type=int)
    args_parser.add_argument('--min_devices_number', default=1, help='Minimal devices number',
                             dest='min_devices_number', type=int)
    args_parser.add_argument('--max_devices_number', default=5, help='Maximal devices number',
                             dest='max_devices_number', type=int)
    args_parser.add_argument('--sink', default='both', help='Data sink (possible redis, file or both)', dest='sink', type=str)
    args_parser.add_argument('--redis_port', default=6060, help='Port on which redis runs', dest='redis_port', type=str)
    args_parser.add_argument('--redis_host', default='redis', help='Host on which redis runs', dest='redis_host',
                             type=str)
    args_parser.add_argument('--users_filepath', default='users.csv', help='Path where to write users',
                             dest='users_filepath', type=str)
    args_parser.add_argument('--items_filepath', default='items.csv', help='Path where to write items',
                             dest='items_filepath', type=str)
    args_parser.add_argument('--ips_filepath', default='ips.csv', help='Path where to write ips', dest='ips_filepath',
                             type=str)
    args_parser.add_argument('--countries_filepath', default='resources/countries.txt',
                             help='Path to a file with possible countries', dest='countries_filepath', type=str)
    args_parser.add_argument('--devices_filepath', default='resources/devices.txt',
                             help='Path to a file with possible devices', dest='devices_filepath', type=str)

    return args_parser.parse_args()


def set_up_logging() -> None:
    """
    Sets up logging
    """
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)


def generate_data(args: argparse.Namespace) -> tuple:
    """
    Generates data
    :param args: cli arguments
    :return: tuple with generated data
    """
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
    """
    Writes data to a selected sink
    :param args: cli arguments
    :param users: list of users to write
    :param ips: list of ips to write
    :param items: list of items to write
    """
    logging.info("Started writing data")

    if args.sink.lower() == "redis":
        redis_connector = RedisConnector.RedisConnector(args.redis_host, args.redis_port)
        redis_connector.write(users, ips, items)
    elif args.sink.lower() == "file":
        FileConnector.write_users(args.users_filepath, users)
        FileConnector.write_ips(args.ips_filepath, ips)
        FileConnector.write_items(args.items_filepath, items)
    elif args.sink.lower() == "both":
        FileConnector.write_users(args.users_filepath, users)
        FileConnector.write_ips(args.ips_filepath, ips)
        FileConnector.write_items(args.items_filepath, items)
        redis_connector = RedisConnector.RedisConnector(args.redis_host, args.redis_port)
        redis_connector.write(users, ips, items)
    else:
        raise ValueError("Sink must be redis, file or both")

    logging.info("Finished writing data")


def __main__():
    set_up_logging()
    args = parse_arguments()
    load(args.devices_filepath, args.countries_filepath)
    data = generate_data(args)
    write_data(args, *data)


if __name__ == "__main__":
    __main__()
