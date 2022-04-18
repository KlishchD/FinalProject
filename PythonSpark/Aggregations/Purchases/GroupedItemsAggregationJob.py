from pyspark.sql import DataFrame
from pyspark.sql.functions import column

from Aggregations.Purchases.PurchasesAggregationJob import PurchasesAggregationJob
from Utils.Parsing import count_data_frame_columns


class GroupedItemsAggregationJob(PurchasesAggregationJob):
    """
    This class calculates a number of times two items are bought together.
    Result is a dataframe with columns:
     item1 - first item
     item2 - second
     count - number item1 and item2 were bought together in specified locations, devices and time frame
    """

    def process(self, data: dict) -> DataFrame:
        filtered_purchases_with_locations = super(GroupedItemsAggregationJob, self).process(data)

        first_group = GroupedItemsAggregationJob.__select_item_group__(filtered_purchases_with_locations, "item1")
        second_group = GroupedItemsAggregationJob.__select_item_group__(filtered_purchases_with_locations, "item2")

        joined = first_group.join(second_group, "user_id", "ts")

        return count_data_frame_columns(joined, ["item1", "item2"])

    @staticmethod
    def __select_item_group__(data: DataFrame, new_item_id_column_name: str) -> DataFrame:
        return data.select("user_id", "ts", column("item_id").alias(new_item_id_column_name))
