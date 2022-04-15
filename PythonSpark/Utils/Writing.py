import logging

from pyspark.sql import DataFrame


def write_to_parquet(data: DataFrame, filepath: str) -> None:
    logging.info(f"Started writing data in parquet format to {filepath}")
    data.write.parquet(filepath)
    logging.info(f"Finished writing data in parquet format to {filepath}")


def write_to_postgres(data: DataFrame, url: str, user: str, password: str, table: str) -> None:
    logging.info(f"Started writing data to postgres with url {url} to a {table} table")
    data.write \
        .format("jdbc") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", table) \
        .save()
    logging.info(f"Finished writing data to postgres with url {url} to a {table} table")


def write_to_big_query(data: DataFrame,
                       service_account_key_filepath: str,
                       temporary_bucket_name: str,
                       table: str) -> None:
    logging.info(f"Started writing data to BigQuery to a {table} table")
    data.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", temporary_bucket_name) \
        .option("credentialsFile", service_account_key_filepath) \
        .save(table)
    logging.info(f"Finished writing data to BigQuery to a {table} table")
