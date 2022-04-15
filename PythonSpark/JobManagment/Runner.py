from abc import ABC, abstractmethod
from typing import Type

from JobManagment.Job import Job


class Runner(ABC):
    @abstractmethod
    def register(self, job_name: str, job_class: Type[Job]):
        pass

    @abstractmethod
    def run(self, job_name: str, spark_master: str, spark_app_name: str, args: list):
        pass
