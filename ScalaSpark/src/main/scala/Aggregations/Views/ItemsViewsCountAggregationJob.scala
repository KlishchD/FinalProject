package Aggregations.Views

import JobManagment.JobCompanion
import Utils.Parsing.countColumn
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * This class calculates a number of time items was viewed
 * Result is a dataframe with columns:
 *   item_id - id of the item
 *   count - number of times item was viewed in specified locations, devices and time frame
 */
class ItemsViewsCountAggregationJob(arguments: Map[String, String], spark: SparkSession) extends ViewsAggregationJob(arguments, spark) {

  override def process(data: Map[String, DataFrame]): DataFrame = {
    countColumn(super.process(data), "item_id", "count")
  }
}

object ItemsViewsCountAggregationJob extends JobCompanion {
  override def parser(): Parser = ViewsAggregationJob.parser()
}