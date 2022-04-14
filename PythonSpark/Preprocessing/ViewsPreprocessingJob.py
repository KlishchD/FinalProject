from pyspark.sql import DataFrame

from Preprocessing.PreprocessingJob import PreprocessingJob
from Utils.Repartitioning import repartition_data_frame_by_date
from Utils.TimeParsing import convert_string_to_timestamp


class ViewsPreprocessingJob(PreprocessingJob):
    def process(self, data: dict) -> DataFrame:
        views = data["data"]
        time_fixed = convert_string_to_timestamp(views, "ts")
        return repartition_data_frame_by_date(time_fixed, "ts")
