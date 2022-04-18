package Aggregations.Purchases

import Aggregations.AggregationJob
import JobManagment.JobCompanion
import Utils.ArgumentsParsing.RichParser
import Utils.Loading.{loadDynamic, loadStatic}
import Utils.Parsing.filterData
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class PurchasesAggregationJob(arguments: Map[String, String], spark: SparkSession) extends AggregationJob(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    Map(
      "purchases" -> loadDynamic("purchases", arguments, spark),
      "ips" -> loadStatic("ips", arguments, spark)
    )
  }

  override def process(data: Map[String, DataFrame]): DataFrame = {
    val purchasesWithLocations = data("purchases").join(data("ips"), "ip")

    filterData(arguments("timeFrame"), arguments("devices"), arguments("locations"), purchasesWithLocations)
  }
}

object PurchasesAggregationJob extends JobCompanion {
  override def parser(): Parser = {
    AggregationJob.parser()
      .addDynamicTableDataSource("purchases")
      .addStaticTableDataSource("ips")
  }
}
