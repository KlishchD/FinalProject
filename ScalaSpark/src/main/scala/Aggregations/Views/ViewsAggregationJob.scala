package Aggregations.Views

import Aggregations.AggregationJob
import JobManagment.JobCompanion
import Utils.ArgumentsParsing.RichParser
import Utils.Loading.{loadDynamic, loadStatic}
import Utils.Parsing.filterData
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class ViewsAggregationJob(arguments: Map[String, String], spark: SparkSession) extends AggregationJob(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    Map(
      "views" -> loadDynamic("views", arguments, spark),
      "ips" -> loadStatic("ips", arguments, spark)
    )
  }

  override def process(data: Map[String, DataFrame]): DataFrame = {
    val viewsWithLocations = data("views").join(data("ips"), "ip")

    filterData(arguments("timeFrame"), arguments("devices"), arguments("locations"), viewsWithLocations)
  }
}

object ViewsAggregationJob extends JobCompanion {
  override def parser(): Parser = {
    AggregationJob.parser()
      .addDynamicTableDataSource("views")
      .addStaticTableDataSource("ips")
      .addTimeFrame()
      .addDevices()
      .addLocation()
  }
}