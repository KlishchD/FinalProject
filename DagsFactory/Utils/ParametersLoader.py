import json


def load_configs(filepath) -> list:
    try:
        parsed = json.load(open(filepath))
    except FileNotFoundError:
        return []

    result = []

    for key, value in parsed.items():
        if key[0] != "-":
            result.append(str(value))
        else:
            result.append(key + " " + str(value))

    return result
