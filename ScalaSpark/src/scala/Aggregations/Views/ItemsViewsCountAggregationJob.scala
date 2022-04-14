package Aggregations.Views

import JobManagment.JobCompanion
import Utils.Parsing.countColumn
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

class ItemsViewsCountAggregationJob(arguments: Map[String, String], spark: SparkSession) extends ViewsAggregationJob(arguments, spark) {

  override def process(data: Map[String, DataFrame]): DataFrame = {
    countColumn(super.process(data), "item_id", "count")
  }
}

object ItemsViewsCountAggregationJob extends JobCompanion {
  override def parser(): Parser = ViewsAggregationJob.parser()
}