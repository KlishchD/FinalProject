import logging
import os

from google.cloud import storage
from google.oauth2 import service_account


class GCPConnector:
    def __init__(self, key_filepath, ):
        self.__credentials__ = self.__generate_credentials__(key_filepath)

    def write_to_bucket(self, bucket_name, purchases, views):
        """
        Writes views and purchases to the bucket with name bucket_name
        :param bucket_name: name of the bucket to write to
        :param purchases: pandas DataFrame with purchases
        :param views: pandas DataFrame with views
        """
        self.__set_up__(purchases, views)
        self.__write__(bucket_name, "views.csv")
        self.__write__(bucket_name, "purchases.csv")
        self.__clean_up__()

    def __set_up__(self, purchases, views):
        logging.info("Started creating temporary files")
        self.__write_to_file__("views.csv", views)
        self.__write_to_file__("purchases.csv", purchases)
        logging.info("Finished creating temporary files")

    def __write__(self, bucket_name, filepath):
        logging.info(f"Started pushing {filepath} to {bucket_name} bucket")
        client = storage.Client(credentials=self.__credentials__)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(filepath)
        blob.upload_from_filename(filepath)
        logging.info(f"Finished pushing {filepath} to {bucket_name} bucket")

    def __clean_up__(self):
        logging.info("Started cleaning up temporary files")
        os.remove("views.csv")
        os.remove("purchases.csv")
        logging.info("Finished cleaning up temporary files")

    def __generate_credentials__(self, key_filepath):
        return service_account.Credentials.from_service_account_file(key_filepath)

    def __write_to_file__(self, filepath, data, header=True):
        """
        Writes data from pandas DataaFrame to file
        :param header: flag says whether to write header or not
        :param filepath: path where to write a file
        :param data: pandas DataFrame to write
        :return: nothing
        """
        data.to_csv(filepath, index=False, header=header)
