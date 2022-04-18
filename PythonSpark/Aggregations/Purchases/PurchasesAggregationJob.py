from abc import ABC

from pyspark.sql import DataFrame

from Aggregations.AggregationJob import AggregationJob
from Utils.Loading import load_dynamic_table, load_static_table
from Utils.Parsing import filter_data_frame
from Utils.RichArgumentParser import RichArgumentParser


class PurchasesAggregationJob(AggregationJob, ABC):
    def load(self) -> dict:
        return {
            "purchases": load_dynamic_table("purchases", self.arguments, self.spark),
            "ips": load_static_table("ips", self.arguments, self.spark)
        }

    def process(self, data: dict) -> DataFrame:
        joined = data["purchases"].join(data["ips"], "ip")
        return filter_data_frame(joined,
                                 self.arguments.locations,
                                 self.arguments.time_frame,
                                 self.arguments.devices)

    @staticmethod
    def parser() -> RichArgumentParser:
        return AggregationJob.parser() \
            .add_dynamic_table_data_source("purchases") \
            .add_static_table_data_source("ips")
