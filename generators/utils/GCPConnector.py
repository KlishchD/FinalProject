import logging
import os
from typing import Any

import google
import pandas
from google.cloud import storage
from google.oauth2 import service_account

from . import FileConnector


class GCPConnector:
    def __init__(self, key_filepath: str):
        self.__credentials__ = self.__generate_credentials__(key_filepath)

    def write_to_bucket(self,
                        bucket_name: str,
                        purchases: pandas.DataFrame,
                        views: pandas.DataFrame) -> None:
        """
        Writes views and purchases to the bucket with name bucket_name
        :param bucket_name: name of the bucket to write to
        :param purchases: pandas DataFrame with purchases
        :param views: pandas DataFrame with views
        """
        self.__set_up__(purchases, views)
        self.__write__(bucket_name, "views.csv")
        self.__write__(bucket_name, "purchases.json")
        self.__clean_up__()

    def write_to_bucket_files(self,
                              bucket_name: str,
                              purchases_filepath: str,
                              views_filepath: str) -> None:
        """
        Writes views and purchases files to the bucket with name bucket_name
        :param purchases_filepath: path to a file with purchases
        :param views_filepath: path to a file with views
        :param bucket_name: name of the bucket to write to
        """
        self.__write__(bucket_name, views_filepath)
        self.__write__(bucket_name, purchases_filepath)

    def __set_up__(self,
                   purchases: pandas.DataFrame,
                   views: pandas.DataFrame) -> None:
        logging.info("Started creating temporary files")
        FileConnector.write_data_frame_to_csv("views.csv", views)
        FileConnector.write_data_frame_to_json("purchases.json", purchases, index=True)
        logging.info("Finished creating temporary files")

    def __write__(self,
                  bucket_name: str,
                  filepath: str) -> None:
        logging.info(f"Started pushing {filepath} to {bucket_name} bucket")
        client = storage.Client(credentials=self.__credentials__)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(filepath)
        blob.upload_from_filename(filepath)
        logging.info(f"Finished pushing {filepath} to {bucket_name} bucket")

    def __clean_up__(self) -> None:
        logging.info("Started cleaning up temporary files")
        os.remove("views.csv")
        os.remove("purchases.json")
        logging.info("Finished cleaning up temporary files")

    def __generate_credentials__(self,
                                 key_filepath: str) -> Any:
        return service_account.Credentials.from_service_account_file(key_filepath)
