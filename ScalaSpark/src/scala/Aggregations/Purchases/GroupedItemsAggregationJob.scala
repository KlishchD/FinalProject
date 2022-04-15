package Aggregations.Purchases

import JobManagment.JobCompanion
import de.halcony.argparse.Parser
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class calculates a number of times two items are bought together
 * Result is a dataframe with columns:
 *   item1 - first item
 *   item2 - second
 *   count - number item1 and item2 were bought together in specified locations, devices and time frame
 */
class GroupedItemsAggregationJob(arguments: Map[String, String], spark: SparkSession) extends PurchasesAggregationJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val filteredPurchasesWithLocations = super.process(data)

    val firstGroup = selectItemGroupFromPurchases(filteredPurchasesWithLocations, "item1")

    val secondGroup = selectItemGroupFromPurchases(filteredPurchasesWithLocations, "item2")

    val joined = firstGroup.join(secondGroup, "ts", "user_id")

    val filteredOutGroupsWithSameItems = joined.filter(column("item1") > column("item2"))

    filteredOutGroupsWithSameItems.groupBy("item1", "item2").count()
  }

  def selectItemGroupFromPurchases(purchases: DataFrame, newItemIdColumnName: String): DataFrame = {
    purchases.select(column("ts"), column("user_id"), column("item_id").as(newItemIdColumnName))
  }
}

object GroupedItemsAggregationJob extends JobCompanion {
  override def parser(): Parser = PurchasesAggregationJob.parser()
}