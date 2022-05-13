import logging
import pandas

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
