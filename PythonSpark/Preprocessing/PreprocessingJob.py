import argparse
from abc import ABC

from pyspark.sql import DataFrame, SparkSession

from JobManagment.Job import Job
from Utils.Loading import load_data_frame_from_file
from Utils.RichArgumentParser import RichArgumentParser
from Utils.Services import set_up_service_account
from Utils.Writing import write_to_parquet


class PreprocessingJob(Job, ABC):
    def __init__(self, arguments: argparse.Namespace, spark: SparkSession):
        super(PreprocessingJob, self).__init__(arguments, spark)

    def load(self) -> dict:
        if self.arguments.mode == "prod":
            set_up_service_account(self.arguments.read_service_account_email,
                                   self.arguments.read_service_account_key_filepath,
                                   self.spark)

        return {"data": load_data_frame_from_file(self.arguments.input_filepath,
                                                  self.arguments.input_file_format,
                                                  self.spark)}

    def write(self, data: DataFrame) -> None:
        if self.arguments.mode == "prod":
            set_up_service_account(self.arguments.write_service_account_email,
                                   self.arguments.write_service_account_key_filepath,
                                   self.spark)

        write_to_parquet(data, self.arguments.result_filepath)

    @staticmethod
    def parser() -> RichArgumentParser:
        return Job \
            .parser() \
            .add_mode() \
            .add_dynamic_table_data_source("input") \
            .add_file_format_to_data("input") \
            .add_read_service_account() \
            .add_write_service_account() \
            .add_result_filepath()
