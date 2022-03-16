import sys
from random import randint, uniform

# Constants
DEVICES = ["Desktop", "Phone", "Tablet"]
COUNTRIES = ["Ukraine", "United States of America", "United Kingdom"]

# Defaults
ITEMS_TO_GENERATE_DEFAULT = 100_000
MAX_ITEM_ID_DEFAULT = 1_000_000_000
MIN_ITEM_ID_DEFAULT = 0
MIN_ITEM_AMOUNT_DEFAULT = 0
MAX_ITEM_AMOUNT_DEFAULT = 100_000
MIN_ITEM_PRICE_DEFAULT = 0.5
MAX_ITEM_PRICE_DEFAULT = 1000_000.0

USERS_TO_GENERATE_DEFAULT = 100_000
MAX_DEVICES_NUMBER_DEFAULT = 10
MIN_DEVICES_NUMBER_DEFAULT = 1
MIN_USER_ID_DEFAULT = 0
MAX_USER_ID_DEFAULT = 1000_000

# Implementation

used_item_id = {-1}

used_user_id = {-1}
used_ip_address = {"0.0.0.0"}

args = {}


def generate_users_and_ips_file(filepath_users, filepath_ip, users_number=USERS_TO_GENERATE_DEFAULT, **kwargs):
    """
    Generates users and ip files
    :param users_number: number of users to generate
    :param filepath_users: path to a file with users
    :param filepath_ip: path to a file with ips
    :return: nothing
    """
    with open(filepath_users, "w+") as file_users, open(filepath_ip, "w+") as file_ips:
        for i in range(users_number):
            user = generate_user()
            write_user(file_users, user)
            write_devices(file_ips, user[1], user[2])


def generate_items(filepath, items_number=ITEMS_TO_GENERATE_DEFAULT, **kwargs):
    """
    Generates items and writes them to file
    :param items_number: number of items to generate
    :param filepath: path to a file to write items in
    :return: nothing
    """
    with open(filepath, "w+") as file:
        for i in range(items_number):
            write_item_to_file(generate_item(), file)


def generate_user():
    """
    Generates user
    :return: tuple that represents user
    """
    return generate_user_id(**args), generate_devices(**args), generate_country()


def generate_user_id(min_user_id=MIN_USER_ID_DEFAULT, max_user_id=MAX_USER_ID_DEFAULT, **kwargs):
    """
    Generates user id
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: unique user id
    """
    user_id = -1
    while user_id in used_user_id:
        user_id = randint(min_user_id, max_user_id)
    used_user_id.add(user_id)
    return "user" + str(user_id)


def generate_devices(min_devices_number=MIN_DEVICES_NUMBER_DEFAULT, max_devices_number=MAX_DEVICES_NUMBER_DEFAULT,
                     **kwargs):
    """
    Generates devices
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :return: list of tuples that represent devices
    """
    devices = []

    for i in range(randint(min_devices_number, max_devices_number)):
        devices.append((DEVICES[randint(0, len(DEVICES) - 1)], generate_random_ip()))

    return devices


def generate_random_ip():
    """
    Generates random ip that hasn't been generated by it before
    :return: random ip that hasn't been generated by this function before
    """
    blocks = [0, 0, 0, 0]
    while compose_ip(blocks) in used_ip_address:
        blocks = []
        for i in range(4):
            blocks.append(randint(0, 255))

    used_ip_address.add(compose_ip(blocks))

    return compose_ip(blocks)


def generate_country():
    """
    Randomly peeks a country from a list of possible
    :return: country
    """
    return COUNTRIES[randint(0, len(COUNTRIES) - 1)]


def generate_item():
    """
    Generates item
    :return: tuple that represents item
    """
    return generate_item_id(**args), generate_item_amount(**args), generate_item_price(**args)


def generate_item_id(min_item_id=MIN_ITEM_ID_DEFAULT, max_item_id=MAX_ITEM_ID_DEFAULT, **kwargs):
    """
    Generates unique item id
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :return: unique item id
    """
    item_id = -1
    while item_id in used_item_id:
        item_id = randint(min_item_id, max_item_id)

    used_item_id.add(item_id)
    return "item" + str(item_id)


def generate_item_amount(min_item_amount=MIN_ITEM_AMOUNT_DEFAULT, max_item_amount=MAX_ITEM_AMOUNT_DEFAULT, **kwargs):
    """
    Generates item amount
    :param min_item_amount: lowest possible amount of items
    :param max_item_amount: biggest possible amount of items
    :return: item amount
    """
    return randint(min_item_amount, max_item_amount)


def generate_item_price(min_item_price=MIN_ITEM_PRICE_DEFAULT, max_item_price=MAX_ITEM_PRICE_DEFAULT, **kwargs):
    """
    Generates price
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: price
    """
    return uniform(min_item_price, max_item_price)


def compose_ip(blocks):
    """
    Takes list of 4 ints that represent 4 parts of ip address and assembles ip string
    :param blocks: list of 4 ints
    :return: string that represents ip
    """
    ip = str(blocks[0])
    for i in range(3):
        ip += "." + str(blocks[i])
    return ip


def write_user(file, user):
    """
    Writes user to a file
    :param file: opened file to write in
    :param user: user to write in
    :return: nothing
    """
    for device in user[1]:
        file.write(user[0] + "," + device[0] + "," + device[1] + "\n")


def write_devices(file, devices, country):
    """
    Writes list of devices and their country to a file
    :param file: opened file to write in
    :param devices: list of devices to write in
    :param country: country of owner of devices
    :return: nothing
    """
    for device in devices:
        file.write(device[1] + "," + country + "\n")


def write_item_to_file(item, file):
    """
    Writes item to a file
    :param item: item that will be written
    :param file: opened file in which item wil be written
    :return: nothing
    """
    file.write(str(item[0]) + "," + str(item[1]) + "," + str(item[2]) + "\n")


def parse_arguments():
    """
    Parses parameter from CLI
    :return: nothing
    """
    for i in range(1, len(sys.argv), 2):
        args[sys.argv[i]] = int(sys.argv[i + 1])


def __main__():
    parse_arguments()
    generate_users_and_ips_file("users.csv", "ips.csv", **args)
    generate_items("items.csv", **args)


if __name__ == "__main__":
    __main__()