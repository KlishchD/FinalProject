package Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{column, from_unixtime, to_date, to_timestamp}

object Parsing {
  def convertStringToTimeStamp(columnName: String, data: DataFrame): DataFrame = {
    data.withColumn(columnName, to_timestamp(column(columnName)))
  }

  def convertUnixToTimeStamp(columnName: String, data: DataFrame): DataFrame = {
    val unixTime = data.withColumn(columnName, from_unixtime(column(columnName) / 1000))
    convertStringToTimeStamp(columnName, unixTime)
  }

  def repartitionByDate(columnName: String, data: DataFrame): DataFrame = {
    data.repartitionByRange(to_date(column(columnName)))
  }

}
