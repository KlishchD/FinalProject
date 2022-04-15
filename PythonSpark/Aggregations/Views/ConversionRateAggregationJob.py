from pyspark.sql import DataFrame

from Aggregations.Views.ViewsAggregationJob import ViewsAggregationJob
from Utils.Loading import load_dynamic_table
from Utils.Parsing import count_data_frame_column
from Utils.RichArgumentParser import RichArgumentParser


class ConversionRateAggregationJob(ViewsAggregationJob):
    """
    This class calculates a conversion rate (number of times item was bought / number of times of was viewed).
    Result is a dataframe with columns:
     item_id - id of the item
     rate - calculated conversion rate for items in specified locations, devices and time frame
    """

    def load(self) -> dict:
        result = super(ConversionRateAggregationJob, self).load()
        result["purchases"] = load_dynamic_table("purchases", self.arguments, self.spark)
        return result

    def process(self, data: dict) -> DataFrame:
        views_filtered_with_locations = super(ViewsAggregationJob, self).process(data)

        views_counted = count_data_frame_column(views_filtered_with_locations, "item_id", "views_count")
        purchases_counted = count_data_frame_column(data["purchases"], "item_id", "purchases_count")

        joined = purchases_counted.join(views_counted, "item_id")

        return joined.select("item_id", "purchases_count / views_count")

    @staticmethod
    def parser() -> RichArgumentParser:
        return ViewsAggregationJob.parser() \
            .add_dynamic_table_data_source("purchases")
