from abc import ABC

from pyspark.sql import DataFrame

from Aggregations.AggregationJob import AggregationJob
from Utils.Loading import load_dynamic_table, load_static_table
from Utils.Parsing import filter_data_frame


class ViewsAggregationJob(AggregationJob, ABC):
    def load(self) -> dict:
        return {
            "views": load_dynamic_table("views", self.arguments, self.spark),
            "ips": load_static_table("ips", self.arguments, self.spark)
        }

    def process(self, data: dict) -> DataFrame:
        joined = data["views"].join(data["ips"], "ip")
        return filter_data_frame(joined,
                                 self.arguments.location,
                                 self.arguments.time,
                                 self.arguments.devices)
