from datetime import datetime, timedelta

from dateutil import parser
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, column, sum


def explode_data_frame_column(data: DataFrame, column_name: str) -> DataFrame:
    return data.withColumn(column_name, explode(column_name))


def extract_array_element_to_new_column(data: DataFrame,
                                        array_column_name: str,
                                        item_index: int,
                                        new_column_name: str) -> DataFrame:
    return data.withColumn(new_column_name, column(array_column_name).getItem(item_index))


def count_data_frame_column(data: DataFrame, column_name: str, result_column_name: str = "count") -> DataFrame:
    return data.groupby(column_name).agg(sum("*").alias(result_column_name))


def count_data_frame_columns(data: DataFrame, column_names: list, result_column_name: str = "count") -> DataFrame:
    return data.groupby(*column_names).agg(sum("*").alias(result_column_name))


def count_data_frame_column_total_value(data: DataFrame, column_name: str) -> int:
    return data.select(sum(column_name).alias("count")).first.__getitem__("count")


def calculate_share(data: DataFrame,
                    grouping_column_name: str,
                    value_column_name: str,
                    result_column_name: str = "share") -> DataFrame:
    total = count_data_frame_column_total_value(data, value_column_name)

    return data.groupby(grouping_column_name).agg((column(value_column_name) / total).alias(result_column_name))


def filter_data_frame(data: DataFrame, location: str, time: str, devices: str) -> DataFrame:
    filtered_by_time = filter_data_frame_by_time(data, time)
    filtered_by_location = filter_data_frame_by_location(filtered_by_time, location)
    return filter_data_frame_by_time(filtered_by_location, devices)


def filter_data_frame_by_time(data: DataFrame, time: str) -> DataFrame:
    time_parsed = parse_time(time)
    return data.filter(f"ts >= {time_parsed[0]} AND ts <= {time_parsed[1]}")


def filter_data_frame_by_location(data: DataFrame, location: str) -> DataFrame:
    return filter_data_frame_by_string_array(data, "country", location)


def filter_data_frame_by_devices(data: DataFrame, devices: str) -> DataFrame:
    return filter_data_frame_by_string_array(data, "device", devices)


def filter_data_frame_by_string_array(data: DataFrame, column_name: str, array: str) -> DataFrame:
    if array == "None":
        return data

    location = parse_array(array)
    return data.filter(column(column_name).isin(location))


def parse_array(array: str) -> list:
    return array.split(",")


def parse_time(time: str) -> tuple:
    now = datetime.now()
    if time == "day":
        return now - timedelta(days=1), now
    elif time == "week":
        return now - timedelta(days=7), now
    elif time == "month":
        return now - timedelta(weeks=4), now
    elif time == "year":
        return now - timedelta(weeks=52), now
    start, end = time.split("-")
    return parser.parse(start), parser.parse(end)
