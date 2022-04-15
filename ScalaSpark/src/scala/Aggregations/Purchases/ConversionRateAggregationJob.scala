package Aggregations.Purchases

/**
 * This class calculates a conversion rate (number of times item was bought / number of times of was viewed)
 * Result is a dataframe with columns:
 *   item_id - id of the item
 *   rate - calculated conversion rate for items in specified locations, devices and time frame
 */
class ConversionRateAggregationJob(arguments: Map[String, String], spark: SparkSession) extends PurchasesAggregationJob(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    super.load() + ("views" -> loadDynamic("views", arguments, spark))
  }

  override def process(data: Map[String, DataFrame]): DataFrame = {
    val filteredPurchasesWithLocations = super.process(data)

    val purchasesCounted = countColumn(filteredPurchasesWithLocations, "item_id", "purchasesCount")

    val viewsCounted = countColumn(data("views"), "item_id", "viewsCount")

    val joined = viewsCounted.join(purchasesCounted, Seq("item_id"), "left")

    val fixedNulls = joined.na.fill(0)

    fixedNulls.select(column("item_id"), expr("purchasesCount / viewsCount").as("rate"))
  }
}

object ConversionRateAggregationJob extends JobCompanion {
  override def parser(): Parser = {
    PurchasesAggregationJob.parser()
      .addDynamicTableDataSource("views")
  }
}