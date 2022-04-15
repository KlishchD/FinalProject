from pyspark.sql import DataFrame

from Preprocessing.PreprocessingJob import PreprocessingJob
from Utils.Repartitioning import repartition_data_frame_by_date
from Utils.TimeParsing import convert_string_to_timestamp


class ViewsPreprocessingJob(PreprocessingJob):
    """
    This class prepares views for further processing (fixes time).
    Result is dataframe with columns:
     user_id - id of a user, who made this view
     ip - ip of a device, that was used to make this view
     device - type of device, that was used to make this view
     item_id - id of an item, that was viewed
     ts - time when a view was made
    """
    def process(self, data: dict) -> DataFrame:
        views = data["data"]
        time_fixed = convert_string_to_timestamp(views, "ts")
        return repartition_data_frame_by_date(time_fixed, "ts")
