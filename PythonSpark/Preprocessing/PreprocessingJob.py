import argparse
from abc import ABC

from pyspark.sql import DataFrame, SparkSession

from JobManagment.Job import Job
from Utils.Loading import load_data_frame_from_file
from Utils.Services import set_up_service_account
from Utils.Writing import write_to_parquet


class PreprocessingJob(Job, ABC):
    def __init__(self, arguments: argparse.Namespace, spark: SparkSession):
        super(PreprocessingJob, self).__init__(arguments, spark)

    def load(self) -> dict:
        input_filepath = self.arguments.datafp
        input_file_format = self.arguments.dataf

        if self.arguments.mode == "prod":
            set_up_service_account(self.arguments.read_service_account_email,
                                   self.arguments.read_service_account_key_filepath,
                                   self.spark)

        return {
            "data": load_data_frame_from_file(input_filepath, input_file_format, self.spark)
        }

    def write(self, data: DataFrame) -> None:
        if self.arguments.mode == "prod":
            set_up_service_account(self.arguments.write_service_account_email,
                                   self.arguments.write_service_account_key_filepath,
                                   self.spark)

        write_to_parquet(data, self.arguments.result_filepath)
