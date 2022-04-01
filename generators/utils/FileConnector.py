import logging


def write_users(filepath: str,
                users: list,
                header: bool = True) -> None:
    """
    Writes list of users to file with filepath path
    :param filepath: path where to write users
    :param users: list of users
    :param header: flag says whether to write header or not
    """
    logging.info('Started writing users')

    with open(filepath, "w") as file:
        if header:
            file.write("user_id,device_name,ip\n")
        for user in users:
            for device in user[1]:
                file.write(",".join([user[0], device[0], device[1]]) + "\n")

    logging.info('Finished writing users')


def write_ips(filepath: str,
              ips: list,
              header: bool = True) -> None:
    """
    Writes a list of ips to a file with filepath path
    :param filepath: path where to write ips
    :param ips: list of ips
    :param header: flag says whether to write header or not
    """
    logging.info('Started writing ips')

    with open(filepath, "w") as file:
        if header:
            file.write("ip,country\n")
        for ip in ips:
            file.write(",".join(ip) + "\n")

    logging.info('Finished writing ips')


def write_items(filepath: str,
                items: list,
                header: bool = True) -> None:
    """
    Writes items to a file with filepath path
    :param filepath: path where to write items
    :param items: list of items
    :param header: flag says whether to write header or not
    """

    logging.info('Started writing items')

    with open(filepath, "w") as file:
        if header:
            file.write("item_id,price\n")
        for item in items:
            file.write(",".join(item) + "\n")

    logging.info('Finished writing items')


def write_DataFrame_to_csv(filepath, data, index=False, header=True) -> None:
    """
    Writes data from pandas DataFrame to csv file
    :param index: flag says whether to write indexes or not
    :param header: flag says whether to write header or not
    :param filepath: path where to write a file
    :param data: pandas DataFrame to write
    """
    logging.info(f"Started writing {filepath}")
    data.to_csv(filepath, index=index, header=header)
    logging.info(f"Finished writing {filepath}")


def write_DataFrame_to_json(filepath, data, index=False) -> None:
    """
    Writes data from pandas DataFrame to json file
    :param index: flag says whether to write indexes or not
    :param filepath: path where to write a file
    :param data: pandas DataFrame to write
    """
    logging.info(f"Started writing {filepath}")
    data.to_json(filepath, index=index, orient="records")
    logging.info(f"Finished writing {filepath}")
