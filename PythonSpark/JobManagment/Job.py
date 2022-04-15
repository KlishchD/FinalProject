import argparse
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from Utils.RichArgumentParser import RichArgumentParser


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
    def parser() -> RichArgumentParser:
        return RichArgumentParser("Job")
