from abc import ABC

from pyspark.sql import DataFrame

from JobManagment.Job import Job
from Utils.RichArgumentParser import RichArgumentParser
from Utils.Writing import write_to_postgres, write_to_big_query


class AggregationJob(Job, ABC):
    def write(self, data: DataFrame) -> None:
        if self.arguments.mode == "dev":
            write_to_postgres(data,
                              self.arguments.postgres_url,
                              self.arguments.postgres_user,
                              self.arguments.postgres_password,
                              self.arguments.result_table)
        else:
            write_to_big_query(data,
                               self.arguments.big_query_service_account_key_filepath,
                               self.arguments.temporary_bucket_name,
                               self.arguments.result_table)

    @staticmethod
    def parser() -> RichArgumentParser:
        return Job.parser() \
            .add_postgres() \
            .add_big_query() \
            .add_result_table()
