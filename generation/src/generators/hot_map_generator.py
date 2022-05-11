import argparse
import pandas
import random
import sys
from datetime import datetime
from kafka import KafkaProducer
from time import sleep

from utils.loading import load
from utils.time import generate_random_timedelta

MOVES = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
TIME_FOR_LAST_GENERATED_SQUARES = {}


def calculate_square_id(square_id: int, move_id: int, squares_number_in_row: int) -> int:
    """
    Calculates next square by applying move on it
    :param square_id: current square id
    :param move_id: id of a move to apply
    :param squares_number_in_row: number of squares in a row
    :return: next square id
    """
    return square_id + MOVES[move_id][0] * squares_number_in_row + MOVES[move_id][1]


def generate_next_square_id(square_id: int, squares_number_in_row: int, squares_number_in_column: int) -> int:
    """
    :param square_id: current square id
    :param squares_number_in_row: number of squares in a row
    :param squares_number_in_column: number of squares in a column
    :return: next square
    """
    new_id = -1
    while new_id < 0 or new_id >= squares_number_in_row * squares_number_in_column:
        move_number = random.randint(0, len(MOVES) - 1)
        new_id = calculate_square_id(square_id, move_number, squares_number_in_row)
    return new_id


def generate_squares_ids_for_view(number: int, squares_number_in_row: int, squares_number_in_column: int) -> list:
    """
    :param number: number of square's ids to generate
    :param squares_number_in_row: number of squares in a row
    :param squares_number_in_column: number of squares in a column
    :return: list with generated square's ids
    """
    result = [random.randint(0, squares_number_in_row * squares_number_in_column - 1)]
    for i in range(number - 1):
        result.append(generate_next_square_id(result[i], squares_number_in_row, squares_number_in_column))
    return result


def generate_incremental_times_for_view(number: int,
                                        start_time: datetime,
                                        min_seconds_delta: int,
                                        max_seconds_delta: int,
                                        min_minutes_delta: int,
                                        max_minutes_delta: int) -> list:
    """
    Generates list of growing times
    :param number: number of times to generate
    :param start_time: starting time
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutes
    :return: list of growing times
    """
    result = [start_time]
    for i in range(number - 1):
        time_delta = generate_random_timedelta(min_seconds_delta, max_seconds_delta,
                                               min_minutes_delta, max_minutes_delta)
        result.append(result[i] + time_delta)
    return result


def build_record(squares_ids: list, times: list, user_id: str, item_id: str) -> list:
    """
    Combines generated data to build list of records, where record looks like (square_id, time, user_id, item_id)
    :param squares_ids: list of squares ids
    :param times: list of times
    :param user_id: id of a user
    :param item_id: in of an item
    :return: list of composed records
    """
    result = []
    for i in range(len(squares_ids)):
        result.append((squares_ids[i], times[i], user_id, item_id))
    return result


def get_iterator_for_values(data: pandas.DataFrame) -> iter:
    """
    :param data: pandas data frame
    :return: iterator on tuples (index, data)
    """
    it = data.iterrows()
    next(it)
    return it


def generate_squares_for_view(view: dict,
                              min_squares_number: int,
                              max_squares_number: int,
                              squares_number_in_row: int,
                              squares_number_in_column: int,
                              min_seconds_delta: int,
                              max_seconds_delta: int,
                              min_minutes_delta: int,
                              max_minutes_delta: int):
    """
    :param view: view as Series
    :param min_squares_number: minimal number of squares to generate for a view
    :param max_squares_number: maximal number of squares to generate for a view
    :param squares_number_in_row: number of squares in a row
    :param squares_number_in_column: number of squares in a column
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutest
    :return: generated squares for all views
    """
    squares_number = random.randint(min_squares_number, max_squares_number)

    squares_ids = generate_squares_ids_for_view(squares_number, squares_number_in_row, squares_number_in_column)

    if (view["user_id"], view["ts"]) not in TIME_FOR_LAST_GENERATED_SQUARES:
        parsed_time = datetime.strptime(view["ts"][:-7].strip(), "%Y-%m-%d %H:%M:%S")
        TIME_FOR_LAST_GENERATED_SQUARES[(view["user_id"], view["ts"])] = parsed_time

    times = generate_incremental_times_for_view(squares_number,
                                                TIME_FOR_LAST_GENERATED_SQUARES[(view["user_id"], view["ts"])],
                                                min_seconds_delta, max_seconds_delta,
                                                min_minutes_delta, max_minutes_delta)

    TIME_FOR_LAST_GENERATED_SQUARES[(view["user_id"], view["ts"])] = times[-1]

    return build_record(squares_ids, times, view["user_id"], view["item_id"])


def generate_squares(views: pandas.DataFrame,
                     sample_size: int,
                     min_squares_number: int,
                     max_squares_number: int,
                     squares_number_in_row: int,
                     squares_number_in_column: int,
                     min_seconds_delta: int,
                     max_seconds_delta: int,
                     min_minutes_delta: int,
                     max_minutes_delta: int) -> list:
    """
    :param sample_size: number of views, which will be used in generation
    :param views: pandas DataFrame with views
    :param min_squares_number: minimal number of squares to generate for a view
    :param max_squares_number: maximal number of squares to generate for a view
    :param squares_number_in_row: number of squares in a row
    :param squares_number_in_column: number of squares in a column
    :param min_seconds_delta: lowest possible amount of seconds
    :param max_seconds_delta: biggest possible amount of seconds
    :param min_minutes_delta: lowest possible amount of minutes
    :param max_minutes_delta: biggest possible amount of minutes
    """
    result = []
    for view in views.sample(sample_size).to_dict(orient="records"):
        result += generate_squares_for_view(view,
                                            min_squares_number, max_squares_number,
                                            squares_number_in_row, squares_number_in_column,
                                            min_seconds_delta, max_seconds_delta,
                                            min_minutes_delta, max_minutes_delta)
    return result


def parse_arguments(arguments: list) -> argparse.Namespace:
    """
    Parses arguments from CLI
    :return: parsed arguments
    """
    args_parser = argparse.ArgumentParser(description='Streaming')
    args_parser.add_argument("--generation_delay",
                             default=0.5,
                             help="Delay between iterations",
                             dest="generation_delay",
                             type=float)
    args_parser.add_argument("--views_filepath",
                             default="views.csv",
                             help="Path to a file with views",
                             dest="views_filepath",
                             type=str)
    args_parser.add_argument("--min_squares_number",
                             default=1,
                             help="Minimal number of squares to generate for one view",
                             dest="min_squares_number",
                             type=int)
    args_parser.add_argument("--max_squares_number",
                             default=100,
                             help="Maximal number of squares to generate for one view",
                             dest="max_squares_number",
                             type=int)
    args_parser.add_argument("--sample_size",
                             default=2,
                             help="Number of views, that will be used at each iteration",
                             dest="sample_size",
                             type=int)
    args_parser.add_argument("--squares_number_in_row",
                             default=1000,
                             help="Number of squares in row",
                             dest="squares_number_in_row",
                             type=int)
    args_parser.add_argument("--squares_number_in_column",
                             default=1000,
                             help="Number of squares in column",
                             dest="squares_number_in_column",
                             type=int)
    args_parser.add_argument("--min_seconds_delta",
                             default=0,
                             help="Minimal delta between seconds",
                             dest="min_seconds_delta",
                             type=int)
    args_parser.add_argument("--max_seconds_delta",
                             default=60,
                             help="Maximal delta between seconds",
                             dest="max_seconds_delta",
                             type=int)
    args_parser.add_argument("--min_minutes_delta",
                             default=0,
                             help="Minimal delta between minutes",
                             dest="min_minutes_delta",
                             type=int)
    args_parser.add_argument("--max_minutes_delta",
                             default=60,
                             help="Maximal delta between minutes",
                             dest="max_minutes_delta",
                             type=int)
    args_parser.add_argument("--topic",
                             default="hot_map_topic",
                             help="Data will be written to specified topic",
                             dest="topic",
                             type=str)
    args_parser.add_argument("--bootstrap_server",
                             default="localhost:9092",
                             help="Kafka bootstrap server address",
                             dest="bootstrap_server",
                             type=str)
    return args_parser.parse_args(arguments)


def write_to_kafka(squares: list, topic: str, producer: KafkaProducer) -> None:
    for row in squares:
        square_id = str(row[0])
        time = row[1].strftime('%Y:%m:%d %H:%M:%S')
        user_id = row[2]
        item_id = row[3]
        producer.send(topic, key=",".join([square_id, item_id]).encode(), value=(",".join([user_id, time])).encode())


def __main__():
    args = parse_arguments(sys.argv[1:])

    views = load(args.views_filepath, ["user_id", "device", "ip", "item_id", "ts"])

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_server)

    while True:
        squares = generate_squares(views,
                                   args.sample_size,
                                   args.min_squares_number, args.max_squares_number,
                                   args.squares_number_in_row, args.squares_number_in_column,
                                   args.min_seconds_delta, args.max_seconds_delta,
                                   args.min_minutes_delta, args.max_minutes_delta)

        write_to_kafka(squares, args.topic, producer)

        sleep(args.generation_delay)


if __name__ == "__main__":
    __main__()
