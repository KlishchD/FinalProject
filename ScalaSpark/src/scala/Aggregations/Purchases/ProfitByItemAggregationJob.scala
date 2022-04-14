package Aggregations.Purchases

import JobManagment.JobCompanion
import Utils.ArgumentsParsing.RichParser
import Utils.Loading.loadStatic
import de.halcony.argparse.Parser
import org.apache.spark.sql.functions.{column, expr, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}


class ProfitByItemAggregationJob(arguments: Map[String, String], spark: SparkSession) extends PurchasesAggregationJob(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    super.load() + ("items" -> loadStatic("items", arguments, spark))
  }

  override def process(data: Map[String, DataFrame]): DataFrame = {
    val filteredPurchasesWithLocations = super.process(data)

    val countedEachItemsAmount = filteredPurchasesWithLocations.groupBy("item_id").agg(sum("amount").as("amount"))

    val purchasesWithItemsPrices = countedEachItemsAmount.join(data("items"), "item_id")

    purchasesWithItemsPrices.select(column("item_id"), expr("price * amount").as("profit"))
  }
}

object ProfitByItemAggregationJob extends JobCompanion {
  override def parser(): Parser = {
    PurchasesAggregationJob.parser()
      .addStaticTableDataSource("items")
  }
}