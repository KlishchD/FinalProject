import argparse

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *


def parse_arguments(args) -> argparse.Namespace:
    args_parser = argparse.ArgumentParser("Streaming job")
    args_parser.add_argument("--spark_master", dest="spark_master", help="Spark Master", default="local[*]", type=str)
    args_parser.add_argument("--spark_app_name", dest="spark_app_name", help="Spark app name", default="app", type=str)
    args_parser.add_argument("--bootstrap_server", dest="bootstrap_server", help="kafka bootstrap server",
                             default="localhost:9092", type=str)
    args_parser.add_argument("--topic", dest="topic", help="Topic name", default="streaming", type=str)

    return args_parser.parse_args(args)


def load(bootstrap_server: str, topic: str, spark: SparkSession) -> DataFrame:
    return spark.readStream \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", topic) \
        .format("kafka") \
        .load()


def parse_key(raw: DataFrame) -> DataFrame:
    decoded = raw.select(column("key").cast("string").alias("key"), column("timestamp"))

    exploded = decoded.select(split(column("key"), ",").alias("key"), "timestamp")

    return exploded.select(column("key").getItem(0).alias("square_id"),
                           column("key").getItem(1).alias("item_id"),
                           column("timestamp"))


def window(data: DataFrame) -> DataFrame:
    return data \
        .withWatermark("timestamp", "5 seconds") \
        .groupBy(column("square_id"), column("item_id"), functions.window(column("timestamp"), "1 minute", "5 seconds"))


def aggregate(data: DataFrame) -> DataFrame:
    return data.count()


def write(data: DataFrame):
    data.writeStream.outputMode("update").format("console").start().awaitTermination()


def __main__():
    args = parse_arguments(sys.argv[1:])

    spark = SparkSession.builder.master(args.spark_master).appName(args.spark_app_name).getOrCreate()

    spark.sparkContext.setLogLevel('error')

    raw = load(args.bootstrap_server, args.topic, spark)

    parsed = parse_key(raw)

    windowed = window(parsed)

    aggregated = aggregate(windowed)

    write(aggregated)


if __name__ == "__main__":
    __main__()
