package Aggregations.Views

import JobManagment.JobCompanion
import Utils.Parsing.{countColumn, countShare}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class calculates a share of items views
 * Result is a dataframe with columns:
 * item_id - id of the item
 * share - percentage of views item generated in specified locations, devices and time frame
 */
class ItemViewsShareAggregationJob(arguments: Map[String, String], spark: SparkSession) extends ViewsAggregationJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val itemsCounted = countColumn(super.process(data), "item_id", "count")

    countShare(itemsCounted, "item_id", "count", "share")
  }

}

object ItemViewsShareAggregationJob extends JobCompanion {
  override def parser(): Parser = ViewsAggregationJob.parser()
}