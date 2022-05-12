from pyspark.sql import functions
from pyspark.sql.functions import *

from JobManagment.Job import Job
from Utils import RichArgumentParser


class HotMapAggregation(Job):
    def load(self) -> dict:
        data = self.spark \
            .readStream \
            .option("kafka.bootstrap.servers", self.arguments.bootstrap_server) \
            .option("subscribe", self.arguments.topic) \
            .format("kafka") \
            .load()
        return {"data": data}

    @staticmethod
    def __parse_key__(raw: DataFrame) -> DataFrame:
        decoded = raw.select(column("key").cast("string").alias("key"), column("timestamp"))

        exploded = decoded.select(split(column("key"), ",").alias("key"), "timestamp")

        return exploded.select(column("key").getItem(0).alias("square_id"),
                               column("key").getItem(1).alias("item_id"),
                               column("timestamp"))

    def process(self, data: dict) -> DataFrame:
        parsed = self.__parse_key__(data["data"])

        grouped = parsed.withWatermark("timestamp", "5 seconds") \
            .groupBy(column("square_id"),
                     column("item_id"),
                     functions.window(column("timestamp"), "1 minute", "5 seconds"))

        return grouped.count()

    def write(self, data: DataFrame) -> None:
        data.writeStream.outputMode("update").format("console").start().awaitTermination()

    @staticmethod
    def parser() -> RichArgumentParser:
        return Job.parser() \
            .add_argument("--spark_master", dest="spark_master", help="Spark Master", default="local[*]", type=str) \
            .add_argument("--spark_app_name", dest="spark_app_name", help="Spark app name", default="app", type=str) \
            .add_argument("--bootstrap_server", dest="bootstrap_server", help="kafka bootstrap server",
                          default="localhost:9092", type=str) \
            .add_argument("--topic", dest="topic", help="Topic name", default="streaming", type=str)
