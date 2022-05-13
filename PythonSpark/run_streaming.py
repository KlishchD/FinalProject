import sys

from JobManagment.JobRunner import JobRunner
from Streaming.HotMapAggregation import HotMapAggregation
from Utils.Services import set_up_logging


def __main__():
    set_up_logging()

    runner = JobRunner()

    runner.register("hotMap", HotMapAggregation)

    job_name = sys.argv[1]
    spark_master = sys.argv[2]
    spark_app_name = sys.argv[3]
    arguments = sys.argv[4:]

    runner.run(job_name, spark_master, spark_app_name, arguments)


if __name__ == "__main__":
    __main__()


