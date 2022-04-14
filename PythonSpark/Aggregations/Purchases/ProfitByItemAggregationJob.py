from pyspark.pandas import DataFrame
from pyspark.sql.functions import expr

from Aggregations.Purchases.PurchasesAggregationJob import PurchasesAggregationJob
from Utils.Loading import load_static_table


class ProfitByItemAggregationJob(PurchasesAggregationJob):
    def load(self) -> dict:
        result = super(ProfitByItemAggregationJob, self).load()
        result["items"] = load_static_table("items", self.arguments, self.spark)
        return result

    def process(self, data: dict) -> DataFrame:
        filtered_purchases_with_locations = super(ProfitByItemAggregationJob, self).process(data)

        items_counted = filtered_purchases_with_locations.groupby("item_id").sum("amount")

        joined = items_counted.join(data["items"], "item_id")

        return joined.select("item_id", expr("amount * price").alias("profit"))
