from typing import Type

from pyspark.sql import SparkSession

from JobManagment.Job import Job
from JobManagment.Runner import Runner


class JobRunner(Runner):
    __registered__ = {}

    def register(self, job_name: str, job_class: Type[Job]):
        self.__registered__[job_name] = job_class

    def run(self, job_name: str, spark_master: str, spark_app_name: str, args: list):
        job_class = self.__registered__[job_name]

        arguments = job_class.parser().parse_args(args)

        spark = SparkSession.builder.master(spark_master).appName(spark_app_name).getOrCreate()

        job = job_class(arguments, spark)

        data = job.load()

        result = job.process(data)

        job.write(result)
