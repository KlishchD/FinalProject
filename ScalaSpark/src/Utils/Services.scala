package Utils

import org.apache.spark.sql.SparkSession

object Services {
  def setUpServiceAccount(serviceAccountEmail: String, serviceAccountKeyFilepath: String, spark: SparkSession): Unit = {
    spark.conf.set("google.auth.service.account.enable", "true")
    spark.conf.set("google.auth.service.account.email", serviceAccountEmail)
    spark.conf.set("google.auth.service.account.keyfile", serviceAccountKeyFilepath)
  }
}
