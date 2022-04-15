from pyspark.pandas import DataFrame

from Aggregations.Views.ViewsAggregationJob import ViewsAggregationJob
from Utils.Parsing import count_data_frame_column


class ItemsViewsCountAggregationJob(ViewsAggregationJob):
    """
    This class calculates a number of time items was viewed.
    Result is a dataframe with columns:
     item_id - id of the item
     count - number of times item was viewed in specified locations, devices and time frame
    """

    def process(self, data: dict) -> DataFrame:
        views_filtered_with_locations = super(ItemsViewsCountAggregationJob, self).process(data)

        return count_data_frame_column(views_filtered_with_locations, "item_id")
