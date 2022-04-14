import argparse
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class Job(ABC):
    def __init__(self, arguments: argparse.Namespace, spark: SparkSession):
        self.arguments = arguments
        self.spark = spark

    @abstractmethod
    def load(self) -> dict:
        pass

    @abstractmethod
    def process(self, data: dict) -> DataFrame:
        pass

    @abstractmethod
    def write(self, data: DataFrame) -> None:
        pass

    @staticmethod
    def parser() -> argparse.ArgumentParser:
        return argparse.ArgumentParser("Job")
