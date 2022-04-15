import logging
import sys

from JobManagment.JobRunner import JobRunner
from Preprocessing.PurchasesPreprocessingJob import PurchasesPreprocessingJob
from Preprocessing.ViewsPreprocessingJob import ViewsPreprocessingJob
from Utils.Services import set_up_logging


def __main__():
    set_up_logging()

    runner = JobRunner()

    runner.register("views", ViewsPreprocessingJob)
    runner.register("purchases", PurchasesPreprocessingJob)

    job_name = sys.argv[1]
    spark_master = sys.argv[2]
    spark_app_name = sys.argv[3]
    arguments = sys.argv[4:]

    runner.run(job_name, spark_master, spark_app_name, arguments)


if __name__ == "__main__":
    __main__()
