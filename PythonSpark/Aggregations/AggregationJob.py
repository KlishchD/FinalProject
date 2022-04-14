from abc import ABC

from pyspark.sql import DataFrame

from JobManagment.Job import Job
from Utils.Writing import write_to_postgres


class AggregationJob(Job, ABC):
    def write(self, data: DataFrame) -> None:
        if self.arguments.mode == "dev":
            write_to_postgres(data,
                              self.arguments.postgres_url,
                              self.arguments.postgres_user,
                              self.arguments.postgres_password,
                              self.arguments.result_table)
