from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date


def repartition_data_frame_by_date(data: DataFrame, time_column_name: str) -> DataFrame:
    return data.repartitionByRange(to_date(time_column_name))
