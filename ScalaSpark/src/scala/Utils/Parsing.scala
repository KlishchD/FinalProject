package Utils

import Utils.ArgumentsParsing.{parseArray, parseTimeFrame}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Parsing {
  def convertUnixToTimeStamp(columnName: String, data: DataFrame): DataFrame = {
    val unixTime = data.withColumn(columnName, from_unixtime(column(columnName) / 1000))
    convertStringToTimeStamp(columnName, unixTime)
  }

  def convertStringToTimeStamp(columnName: String, data: DataFrame): DataFrame = {
    data.withColumn(columnName, to_timestamp(column(columnName)))
  }

  def repartitionByDate(columnName: String, data: DataFrame): DataFrame = {
    data.repartitionByRange(to_date(column(columnName)))
  }

  def countColumn(data: DataFrame, columnName: String, resultColumnName: String): DataFrame = {
    data.groupBy(columnName).agg(count("*").as(resultColumnName))
  }

  def countShare(data: DataFrame, groupingColumnName: String, countsColumnName: String, resultColumnName: String): DataFrame = {
    val total = getSumOfAllValuesInColumn(data, countsColumnName)
    data.groupBy(groupingColumnName).agg((sum(countsColumnName) / total).as(resultColumnName))
  }

  def getSumOfAllValuesInColumn(data: DataFrame, columnName: String): Long = {
    data.agg(sum(columnName)).first().getLong(0)
  }

  def filterData(timeFrame: String, devices: String, locations: String, data: DataFrame): DataFrame = {
    val dataFilteredByTime = filterByTime(timeFrame, "ts", data)
    val dataFilteredByDevices = filterByStringArray(devices, "device", dataFilteredByTime)
    filterByStringArray(locations, "country", dataFilteredByDevices)
  }

  def filterByTime(time: String, columnName: String, data: DataFrame): DataFrame = {
    time match {
      case "None" => data
      case _ =>
        val timeFrame = parseTimeFrame(time)
        data.select("*").where(f"$columnName >= ${timeFrame._1} AND $columnName <= ${timeFrame._2}")
    }
  }

  def filterByStringArray(devices: String, columnName: String, data: DataFrame): DataFrame = {
    devices match {
      case "None" => data
      case _ =>
        val parsed = parseArray(devices)
        data.select("*").where(column(columnName).isin(parsed: _*))
    }
  }

  def explodeDataFrameColumn(columnName: String, data: DataFrame): DataFrame = {
    data.withColumn(columnName, explode(column(columnName)))
  }

  def extractArrayElementToNewColumn(arrayColumnName: String, arrayIndex: Int, newColumnName: String, data: DataFrame): DataFrame = {
    data.withColumn(newColumnName, column(arrayColumnName).getItem(arrayIndex))
  }
}
