package Preprocessing

/**
 * This class prepares purchases for further processing (fixes time and explodes items)
 * Result is a dataframe with columns:
 *   user_id - id of a user, who made a purchase
 *   ip - ip address of a device where using which purchase was made
 *   ts - time when purchase was made
 *   item_id - id of an item purchased
 *   amount - amount of item with item_id purchased
 *   order_id - id of an order itself * @param arguments
 */
class PurchasesPreprocessingJob(arguments: Map[String, String], spark: SparkSession) extends PreprocessingJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val purchases = data("data")

    val timeFixed = convertUnixToTimeStamp("ts", purchases)

    val exploded = explodeDataFrameColumn("items", timeFixed)

    val itemsIdsExtracted = extractArrayElementToNewColumn("items", 0, "item_id", exploded)

    val amountsExtracted = extractArrayElementToNewColumn("items", 1, "amount", itemsIdsExtracted)

    val redundantColumnDropped = amountsExtracted.drop("items")

    repartitionByDate("ts", redundantColumnDropped)
  }
}

object PurchasesPreprocessingJob extends JobCompanion {
  def parser(): Parser = PreprocessingJob.parser()
}
