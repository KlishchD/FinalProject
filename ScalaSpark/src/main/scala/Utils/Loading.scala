package Utils

import Utils.Services.{setUpRedis, setUpServiceAccount}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Loading {

  def loadDynamic(name: String, arguments: Map[String, String], spark: SparkSession): DataFrame = {
    if (arguments("mode") == "prod")
      setUpServiceAccount(arguments("readServiceAccountEmail"), arguments("readServiceAccountKeyFilepath"), spark)

    spark.read.parquet(arguments(f"${name}Filepath"))
  }

  def loadStatic(name: String, arguments: Map[String, String], spark: SparkSession): DataFrame = {
    arguments("mode") match {
      case "dev" => loadCSV(arguments(f"${name}Filepath"), header = true, spark)
      case "prod" =>
        setUpRedis(arguments("redisHost"), arguments("redisPort"), spark)
        loadRedis(arguments(f"${name}KeyPattern"), arguments(f"${name}KeyColumn"), spark)
    }
  }

  def loadCSV(filepath: String, header: Boolean, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", header)
      .csv(filepath)
  }

  def loadRedis(keyPattern: String, keyColumn: String, spark: SparkSession): DataFrame = {
    spark.read
      .format("redis")
      .option("keys.pattern", keyPattern)
      .option("key.column", keyColumn)
      .load()
  }

  def loadFromFile(filepath: String, format: String, spark: SparkSession): DataFrame = {
    if (format.toLowerCase() == "json") loadJSON(filepath, spark)
    else loadCSV(filepath, header = true, spark)
  }

  def loadParquet(filepath: String, spark: SparkSession): DataFrame = {
    spark.read.parquet(filepath)
  }

  def loadJSON(filepath: String, spark: SparkSession): DataFrame = {
    spark.read.json(filepath)
  }
}
