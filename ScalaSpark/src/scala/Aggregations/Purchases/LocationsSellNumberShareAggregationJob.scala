package Aggregations.Purchases

import JobManagment.JobCompanion
import Utils.Parsing.{countColumn, countShare}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

class LocationsSellNumberShareAggregationJob(arguments: Map[String, String], spark: SparkSession) extends PurchasesAggregationJob(arguments, spark) {

  override def process(data: Map[String, DataFrame]): DataFrame = {
    val filteredPurchasesWithLocations = super.process(data)

    val locationsCounted = countColumn(filteredPurchasesWithLocations, "country", "count")

    countShare(locationsCounted, "country", "count", "share")
  }
}

object LocationsSellNumberShareAggregationJob extends JobCompanion {
  override def parser(): Parser = PurchasesAggregationJob.parser()
}