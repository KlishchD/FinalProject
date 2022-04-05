import argparse
import logging

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, to_timestamp, to_date, udf, from_unixtime


def parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Preprocessing')
    parser.add_argument('mode', help='Mode in which app will run (dev or prod)', type=str)
    parser.add_argument('master', help='Spark master', type=str)
    parser.add_argument('views_filepath', help='Path to a file with views', type=str)
    parser.add_argument('purchases_filepath', help='Path to a file with purchases', type=str)
    parser.add_argument('repartitioned_views_filepath', help='Path to a place where to write views', type=str)
    parser.add_argument('repartitioned_purchases_filepath', help='Path to a place where to write purchases', type=str)
    parser.add_argument('--SAKF', dest='service_account_key_filepath',
                        help='Path to a file with service account credentials', type=str)
    parser.add_argument('--SAE', dest='service_account_email', help='Email address of a service account', type=str)

    return parser.parse_args()


def get_spark_session_for_development(master: str) -> SparkSession:
    return SparkSession.builder.master(master).appName("Preprocessing app").getOrCreate()


def set_up_service_account(service_account_email: str, service_account_key_filepath: str, spark: SparkSession) -> None:
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("google.cloud.auth.service.account.email", service_account_email)
    spark.conf.set("google.cloud.auth.service.account.keyfile", service_account_key_filepath)


def get_spark_session_for_production(master: str, service_account_key_filepath: str,
                                     service_account_email: str) -> SparkSession:
    spark = SparkSession.builder.master(master).appName("Preprocessing app").getOrCreate()
    set_up_service_account(service_account_email, service_account_key_filepath, spark)
    return spark


def get_spark_session(args: argparse.Namespace) -> SparkSession:
    if args.mode == 'dev':
        return get_spark_session_for_development(args.master)
    elif args.mode == 'prod':
        return get_spark_session_for_production(args.master, args.service_account_key_filepath,
                                                args.service_account_email)
    else:
        raise ValueError("Deployment mode can be dev or prod")


def load_views(views_filepath: str, spark: SparkSession) -> pyspark.sql.DataFrame:
    raw_views = spark.read.option("header", "true").csv(views_filepath)
    parsed_views = raw_views.select(column("user_id"), column("device"), column("ip"),
                                    to_timestamp(column("ts")).alias("ts"))
    return parsed_views


def repartition_views(views: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return views.repartition(to_date(column("ts")))


def process_views(views_filepath: str, repartitioned_views_filepath: str, spark: SparkSession) -> None:
    logging.info("Started loading views")
    views = load_views(views_filepath, spark)
    logging.info("Finished loading views")

    logging.info("Started views repartitioning")
    repartitioned_views = repartition_views(views)
    logging.info("Finished views repartitioning")

    logging.info("Started writing views")
    repartitioned_views.write.parquet(repartitioned_views_filepath)
    logging.info("Finished writing views")


def load_purchases(purchases_filepath: str, spark: SparkSession) -> pyspark.sql.DataFrame:
    raw_purchases = spark.read.json(purchases_filepath)
    parse_items = udf(lambda raw_list: [[element[0], int(element[1])] for element in raw_list])
    return raw_purchases.select(column("user_id"),
                                column("ip"),
                                parse_items(column("items")).alias("items"),
                                to_timestamp(from_unixtime(column("ts") / 1000, "yyyy-MM-dd HH:mm:ss.SSSS")).alias("ts"))


def repartition_purchases(purchases: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return purchases.repartition(to_date(column("ts")))


def process_purchases(purchases_filepath: str, repartitioned_purchases_filepath: str, spark: SparkSession) -> None:
    logging.info("Started loading purchases")
    purchases = load_purchases(purchases_filepath, spark)
    logging.info("Finished loading purchases")

    logging.info("Started purchase repartitioning")
    repartitioned_purchases = repartition_purchases(purchases)
    logging.info("Finished purchase repartitioning")

    logging.info("Started writing purchases")
    repartitioned_purchases.write.parquet(repartitioned_purchases_filepath)
    logging.info("Finished writing purchases")


def process(views_filepath: str, repartitioned_views_filepath: str, purchases_filepath: str,
            repartitioned_purchases_filepath: str, spark: SparkSession) -> None:
    process_views(views_filepath, repartitioned_views_filepath, spark)
    process_purchases(purchases_filepath, repartitioned_purchases_filepath, spark)


def set_up_logging() -> None:
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)


def __main__():
    args = parse_cli_arguments()

    spark = get_spark_session(args)

    process(args.views_filepath, args.repartitioned_views_filepath,
            args.purchases_filepath, args.repartitioned_purchases_filepath,
            spark)

    spark.stop()


if __name__ == '__main__':
    __main__()
