import logging

from pyspark.sql import SparkSession


def set_up_service_account(service_account_email: str, service_account_key_filepath: str, spark: SparkSession) -> None:
    logging.info(f"Started setting up service account with email {service_account_email}")
    spark.conf.set("google.auth.service.account.enable", "true")
    spark.conf.set("google.auth.service.account.email", service_account_email)
    spark.conf.set("google.auth.service.account.keyfile", service_account_key_filepath)
    logging.info(f"Finished setting up service account with email {service_account_email}")


def set_up_redis(redis_host: str, redis_port: str, spark: SparkSession) -> None:
    logging.info(f"Started setting up redis on {redis_host}:{redis_port}")
    spark.conf.set("spark.redis.host", redis_host)
    spark.conf.set("spark.redis.port", redis_port)
    logging.info(f"Finished setting up redis on {redis_host}:{redis_port}")


def set_up_logging():
    logging.basicConfig(format='%(asctime)s - %(levelname)s [%(name)s] [%(funcName)s():%(lineno)s] - %(message)s',
                        level=logging.INFO)
    logging.getLogger().setLevel("INFO")
