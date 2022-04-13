package Utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Loading {

  def loadFromFile(filepath: String, format: String, spark: SparkSession): DataFrame = {
    spark.read
      .format(format)
      .load(filepath)
  }


}
