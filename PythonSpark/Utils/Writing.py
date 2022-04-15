from pyspark.sql import DataFrame


def write_to_parquet(data: DataFrame, filepath: str) -> None:
    data.write.parquet(filepath)


def write_to_postgres(data: DataFrame, url: str, user: str, password: str, table: str) -> None:
    data.write \
        .format("jdbc") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", table) \
        .save()


def write_to_big_query(data: DataFrame,
                       service_account_key_filepath: str,
                       temporary_bucket_name: str,
                       table: str) -> None:
    data.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", temporary_bucket_name) \
        .option("credentialsFile", service_account_key_filepath) \
        .save(table)
