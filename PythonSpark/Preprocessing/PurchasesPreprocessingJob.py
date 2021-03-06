from pyspark.sql import DataFrame

from Preprocessing.PreprocessingJob import PreprocessingJob
from Utils.Parsing import explode_data_frame_column, extract_array_element_to_new_column
from Utils.Repartitioning import repartition_data_frame_by_date
from Utils.TimeParsing import convert_unix_to_timestamp


class PurchasesPreprocessingJob(PreprocessingJob):
    """
    This class prepares purchases for further processing (fixes time and explodes items).
    Result is a dataframe with columns:
     user_id - id of a user, who made a purchase
     ip - ip address of a device where using which purchase was made
     ts - time when purchase was made
     item_id - id of an item purchased
     amount - amount of item with item_id purchased
     order_id - id of an order itself
    """

    def process(self, data: dict) -> DataFrame:
        time_fixed = convert_unix_to_timestamp(data["data"], "ts")
        purchases = self.__process_purchases__(time_fixed)
        return repartition_data_frame_by_date(purchases, "ts")

    @staticmethod
    def __process_purchases__(purchases: DataFrame) -> DataFrame:
        exploded_purchases = explode_data_frame_column(purchases, "items")
        purchases_with_item_extracted = extract_array_element_to_new_column(exploded_purchases, "items", 0, "item_id")
        purchases_with_amount_extracted = extract_array_element_to_new_column(purchases_with_item_extracted,
                                                                              "items", 1, "amount")
        return purchases_with_amount_extracted.drop("items")
