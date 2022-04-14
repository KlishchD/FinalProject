from pyspark.pandas import DataFrame

from Aggregations.Purchases.PurchasesAggregationJob import PurchasesAggregationJob
from Utils.Parsing import count_data_frame_column, calculate_share


class LocationSellNumberShareAggregation(PurchasesAggregationJob):
    def process(self, data: dict) -> DataFrame:
        filtered_purchases_with_locations = super(LocationSellNumberShareAggregation, self).process(data)

        purchases_counted = count_data_frame_column(filtered_purchases_with_locations, "country")

        return calculate_share(purchases_counted, "country", "count")
