import argparse

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession

from Utils.Services import set_up_service_account, set_up_redis


def load_data_frame_from_file(filepath: str, file_format: str, spark: SparkSession) -> DataFrame:
    return spark.read \
        .format(file_format) \
        .load(filepath)


def load_data_frame_from_redis(keys_pattern: str, key_column: str, spark: SparkSession) -> DataFrame:
    return spark.read \
        .format("org.apache.spark.sql.redis") \
        .option("keys.pattern", keys_pattern) \
        .option("key.column", key_column) \
        .load()


def load_dynamic_table(name: str, arguments: argparse.Namespace, spark: SparkSession) -> DataFrame:
    if arguments.mode == "prod":
        set_up_service_account(arguments.read_serivce_account_email,
                               arguments.read_service_account_key_filepath,
                               spark)

    return load_data_frame_from_file(f"{name}fp", "parquet", spark)


def load_static_table(name: str, arguments: argparse.Namespace, spark: SparkSession) -> DataFrame:
    arguments_str = arguments.__dict__
    if arguments.mode == "prod":
        set_up_redis(arguments.redis_host, arguments.redis_port, spark)
        return load_data_frame_from_redis(arguments_str[f"{name}kp"], arguments_str[f"{name}kc"], spark)

    return load_data_frame_from_file(arguments_str[f"{name}fp"], "csv", spark)
