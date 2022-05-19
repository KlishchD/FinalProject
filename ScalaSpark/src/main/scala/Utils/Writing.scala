package Utils

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object Writing {
  def writeToParquet(data: DataFrame, filepath: String): Unit = {
    data.write.parquet(filepath)
  }

  def writeToPostgres(url: String, table: String, user: String, password: String, data: DataFrame): Unit = {
    val properties = new Properties()

    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", "org.postgresql.Driver")

    data.write
      .mode(SaveMode.Append)
      .jdbc(url, table, properties)
  }

  def writeToBigQuery(temporaryBucketName: String, table: String, keyFilepath: String, data: DataFrame): Unit = {
    data.write
      .format("bigquery")
      .option("temporaryGcsBucket", temporaryBucketName)
      .option("credentialsFile", keyFilepath)
      .save(table)
  }
}
