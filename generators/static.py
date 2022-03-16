import argparse
from random import randint, uniform

# Constants
DEVICES = ["Desktop", "Phone", "Tablet"]
COUNTRIES = ['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla',
             'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan',
             'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda',
             'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina',
             'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria',
             'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands',
             'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands',
             'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica',
             "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti',
             'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea',
             'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France',
             'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia',
             'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala',
             'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands',
             'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia',
             'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan',
             'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Democratic People's Republic of Korea",
             'Republic of Korea', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon',
             'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao',
             'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta',
             'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico',
             'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro',
             'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands',
             'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island',
             'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied',
             'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal',
             'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Rwanda', 'Saint Barthélemy',
             'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia',
             'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa',
             'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone',
             'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia',
             'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
             'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic',
             'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste',
             'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan',
             'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom',
             'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu',
             'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.',
             'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe']

# Implementation

USED_ITEM_ID = {-1}

USED_USER_ID = {-1}
USED_IP_ADDRESSES = {"0.0.0.0"}


def generate_users_and_ips_file(filepath_users, filepath_ip, users_number, min_user_id, max_user_id, min_devices_number,
                                max_devices_number):
    """
    Generates users and ip files
    :param users_number: number of users to generate
    :param filepath_users: path to a file with users
    :param filepath_ip: path to a file with ips
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: nothing
    """
    with open(filepath_users, "w+") as file_users, open(filepath_ip, "w+") as file_ips:
        for i in range(users_number):
            user = generate_user(min_user_id, max_user_id, min_devices_number, max_devices_number)
            write_user(file_users, user)
            write_devices(file_ips, user[1], user[2])


def generate_items(filepath, items_number, min_item_id, max_item_id, min_item_price, max_item_price):
    """
    Generates items and writes them to file
    :param items_number: number of items to generate
    :param filepath: path to a file to write items in
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: nothing
    """
    with open(filepath, "w+") as file:
        for i in range(items_number):
            write_item_to_file(generate_item(min_item_id, max_item_id, min_item_price, max_item_price), file)


def generate_user(min_user_id, max_user_id, min_devices_number, max_devices_number):
    """
    Generates user
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :param min_user_id: lowest possible user id
    :param max_user_id: biggest possible user id
    :return: tuple that represents user
    """
    return generate_user_id(min_user_id, max_user_id), \
           generate_devices(min_devices_number, max_devices_number), \
           generate_country()


def generate_user_id(min_user_id, max_user_id):
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


def generate_devices(min_devices_number, max_devices_number):
    """
    Generates devices
    :param min_devices_number: lowest possible number of devices
    :param max_devices_number: biggest possible number of devices
    :return: list of tuples that represent devices
    """
    return [(DEVICES[randint(0, len(DEVICES) - 1)], generate_random_ip()) for _ in
            range(randint(min_devices_number, max_devices_number))]


def generate_random_ip():
    """
    Generates random ip that hasn't been generated by it before
    :return: random ip that hasn't been generated by this function before
    """
    ip = "0.0.0.0"
    while ip in USED_IP_ADDRESSES:
        ip = ".".join([str(randint(0, 255)) for _ in range(4)])

    USED_IP_ADDRESSES.add(ip)

    return ip


def generate_country():
    """
    Randomly peeks a country from a list of possible
    :return: country
    """
    return COUNTRIES[randint(0, len(COUNTRIES) - 1)]


def generate_item(min_item_id, max_item_id, min_item_price, max_item_price):
    """
    Generates item
    :param min_item_id: lowest possible item id
    :param max_item_id: biggest possible item id
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: tuple that represents item
    """
    return generate_item_id(min_item_id, max_item_id), generate_item_price(min_item_price, max_item_price)


def generate_item_id(min_item_id, max_item_id):
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


def generate_item_price(min_item_price, max_item_price):
    """
    Generates price
    :param min_item_price: lowest possible item price
    :param max_item_price: biggest possible item price
    :return: price
    """
    return str(uniform(min_item_price, max_item_price))


def write_user(file, user):
    """
    Writes user to a file
    :param file: opened file to write in
    :param user: user to write in
    :return: nothing
    """
    for device in user[1]:
        file.write(",".join([user[0], device[0], device[1]]) + "\n")


def write_devices(file, devices, country):
    """
    Writes list of devices and their country to a file
    :param file: opened file to write in
    :param devices: list of devices to write in
    :param country: country of owner of devices
    :return: nothing
    """
    for device in devices:
        file.write(",".join([device[1], country]) + "\n")


def write_item_to_file(item, file):
    """
    Writes item to a file
    :param item: item that will be written
    :param file: opened file in which item wil be written
    :return: nothing
    """
    file.write(",".join(item) + "\n")


def parse_arguments():
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description="Static generator")
    args_parser.add_argument("--items_number", default=10_000, help="Items to generate", dest="items_number")
    args_parser.add_argument("--min_item_id", default=0, help="Minimal item id", dest="min_item_id")
    args_parser.add_argument("--max_item_id", default=100_000_000, help="Maximal item id", dest="max_item_id")
    args_parser.add_argument("--min_item_price", default=0.5, help="Minimal item price", dest="min_item_price")
    args_parser.add_argument("--max_item_price", default=100_000, help="Maximal item price", dest="max_item_price")

    args_parser.add_argument("--users_number", default=10_000, help="Users to generate", dest="users_number")
    args_parser.add_argument("--min_user_id", default=1, help="Minimal user id", dest="min_user_id")
    args_parser.add_argument("--max_user_id", default=10_000_000, help="Maximal user id", dest="max_user_id")

    args_parser.add_argument("--min_devices_number", default=1, help="Minimal devices number",
                             dest="min_devices_number")
    args_parser.add_argument("--max_devices_number", default=5, help="Maximal devices number",
                             dest="max_devices_number")

    return args_parser.parse_args()


def __main__():
    args = parse_arguments()

    generate_users_and_ips_file("users.csv", "ips.csv", args.users_number, args.min_user_id, args.max_user_id,
                                args.min_devices_number, args.max_devices_number)

    generate_items("items.csv", args.items_number, args.min_item_id, args.max_item_id, args.min_item_price,
                   args.max_item_price)


if __name__ == "__main__":
    __main__()
