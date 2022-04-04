import json


def load_configs_for_generator(filepath):
    try:
        parsed = json.load(open(filepath))
    except FileNotFoundError:
        return ""

    result = ""

    for key, value in parsed.items():
        result += " --" + key + " " + str(value)

    return result[1:]
