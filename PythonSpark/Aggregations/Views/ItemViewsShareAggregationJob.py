from pyspark.pandas import DataFrame

from Aggregations.Views.ViewsAggregationJob import ViewsAggregationJob
from Utils.Parsing import calculate_share, count_data_frame_column


class ItemViewsShareAggregationJob(ViewsAggregationJob):
    """
    This class calculates a share of items views.
    Result is a dataframe with columns:
     item_id - id of the item
     share - percentage of views item generated in specified locations, devices and time frame
    """

    def process(self, data: dict) -> DataFrame:
        views_filtered_with_locations = super(ItemViewsShareAggregationJob, self).process(data)

        items_counted = count_data_frame_column(views_filtered_with_locations, "item_id")

        return calculate_share(items_counted, "item_id", "count")
