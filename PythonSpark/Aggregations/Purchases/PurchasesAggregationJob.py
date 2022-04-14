from abc import ABC

from pyspark.sql import DataFrame

from Aggregations.AggregationJob import AggregationJob
from Utils.Loading import load_dynamic_table, load_static_table


class PurchasesAggregationJob(AggregationJob, ABC):
    def load(self) -> dict:
        return {
            "purchases": load_dynamic_table("purchases", self.arguments, self.spark),
            "ips": load_static_table("ips", self.arguments, self.spark)
        }

    def process(self, data: dict) -> DataFrame:
        #TODO: It
        pass