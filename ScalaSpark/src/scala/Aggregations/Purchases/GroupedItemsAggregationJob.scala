package Aggregations.Purchases

import JobManagment.JobCompanion
import de.halcony.argparse.Parser
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.{DataFrame, SparkSession}


class GroupedItemsAggregationJob(arguments: Map[String, String], spark: SparkSession) extends PurchasesAggregationJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val filteredPurchasesWithLocations = super.process(data)

    val first = selectTimeStampAndItemIdFromPurchases(filteredPurchasesWithLocations, "item1")

    val second = selectTimeStampAndItemIdFromPurchases(filteredPurchasesWithLocations, "item2")

    val joined = first.join(second, "ts")

    val filteredOutGroupsWithSameItems = joined.filter(column("item1") > column("item2"))

    filteredOutGroupsWithSameItems.groupBy("item1", "item2").count()
  }

  def selectTimeStampAndItemIdFromPurchases(purchases: DataFrame, newItemIdColumnName: String): DataFrame = {
    purchases.select(column("ts"), column("item_id").as(newItemIdColumnName))
  }
}

object GroupedItemsAggregationJob extends JobCompanion {
  override def parser(): Parser = PurchasesAggregationJob.parser()
}