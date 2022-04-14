package Utils

import org.apache.spark.sql.SparkSession

object Services {
  def setUpServiceAccount(serviceAccountEmail: String, serviceAccountKeyFilepath: String, spark: SparkSession): Unit = {
    spark.conf.set("google.auth.service.account.enable", "true")
    spark.conf.set("google.auth.service.account.email", serviceAccountEmail)
    spark.conf.set("google.auth.service.account.keyfile", serviceAccountKeyFilepath)
  }

  def setUpRedis(redisHost: String, redisPort: String, spark: SparkSession): Unit = {
    spark.conf.set("spark.redis.host", redisHost)
    spark.conf.set("spark.redis.port", redisPort)
  }
}
