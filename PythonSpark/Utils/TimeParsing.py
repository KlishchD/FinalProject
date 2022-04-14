from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, from_unixtime, column


def convert_string_to_timestamp(data: DataFrame, time_column_name: str) -> DataFrame:
    return data.withColumn(time_column_name, to_timestamp(time_column_name))


def convert_unix_to_timestamp(data: DataFrame, time_column_name: str) -> DataFrame:
    converted_to_string_timestamp = data.withColumn(time_column_name, from_unixtime(column(time_column_name) / 1000))
    return convert_string_to_timestamp(converted_to_string_timestamp, time_column_name)
