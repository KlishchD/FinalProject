from pyspark.sql import SparkSession


def set_up_service_account(service_account_email: str, service_account_key_filepath: str, spark: SparkSession) -> None:
    spark.conf.set("google.auth.service.account.enable", "true")
    spark.conf.set("google.auth.service.account.email", service_account_email)
    spark.conf.set("google.auth.service.account.keyfile", service_account_key_filepath)


def set_up_redis(redis_host: str, redis_port: str, spark: SparkSession) -> None:
    spark.conf.set("spark.redis.host", redis_host)
    spark.conf.set("spark.redis.port", redis_port)
