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
