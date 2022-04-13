package JobManagment

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Job(arguments: Map[String, String], spark: SparkSession) {
  def load(): Map[String, DataFrame]

  def process(data: Map[String, DataFrame]): DataFrame

  def write(data: DataFrame): Unit
}
