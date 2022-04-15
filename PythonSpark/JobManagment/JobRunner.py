import logging
from typing import Type

from pyspark.sql import SparkSession

from JobManagment.Job import Job
from JobManagment.Runner import Runner


class JobRunner(Runner):
    __registered__ = {}

    def register(self, job_name: str, job_class: Type[Job]):
        logging.info(f"Stared registering {job_name} job")
        self.__registered__[job_name] = job_class
        logging.info(f"Finished registering {job_name} job")

    def run(self, job_name: str, spark_master: str, spark_app_name: str, args: list):
        logging.info(f"Stared running {job_name} job on a spark with master {spark_master}")
        job_class = self.__registered__[job_name]

        arguments = job_class.parser().parse_args(args)

        spark = SparkSession.builder.master(spark_master).appName(spark_app_name).getOrCreate()

        job = job_class(arguments, spark)
        logging.info("Started loading data")
        data = job.load()
        logging.info("Finished loading data")

        logging.info("Started processing data")
        result = job.process(data)
        logging.info("Finished processing data")

        logging.info("Started writing results")
        job.write(result)
        logging.info("Finished writing results")

        logging.info(f"Finished running {job_name} job on a spark with master {spark_master}")
