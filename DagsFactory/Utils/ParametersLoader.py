import json


def __load_json__(filepath: str) -> dict:
    """
    Loads json file to a dict
    :param filepath: path to a file
    :return: loaded file
    """
    try:
        return json.load(open(filepath))
    except FileNotFoundError:
        return {}


def load_configs(filepath: str) -> list:
    """
    Loads parsed configs
    For property, whose key starts with - returns string:
        key value
    For property, whose key doesn't start with - returns string:
        value
    :param filepath: path to a file to load
    :return: list with parsed arguments
    """

    result = []

    for key, value in __load_json__(filepath).items():
        if key[0] != "-":
            result.append(str(value))
        else:
            result.append(key + " " + str(value))

    return result


def load_raw_file(filepath: str) -> list:
    """
    Loads file to the list
    :param filepath: path to a file to load
    :return: loaded file
    """
    result = []

    try:
        for line in open(filepath):
            if line[-1] == '\n':
                result.append(line[:-1])
            else:
                result.append(line)
    except:
        pass

    return result
