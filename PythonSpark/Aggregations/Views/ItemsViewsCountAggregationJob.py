from pyspark.pandas import DataFrame

from Aggregations.Views.ViewsAggregationJob import ViewsAggregationJob
from Utils.Parsing import count_data_frame_column


class ItemsViewsCountAggregationJob(ViewsAggregationJob):
    def process(self, data: dict) -> DataFrame:
        views_filtered_with_locations = super(ItemsViewsCountAggregationJob, self).process(data)

        return count_data_frame_column(views_filtered_with_locations, "item_id")
