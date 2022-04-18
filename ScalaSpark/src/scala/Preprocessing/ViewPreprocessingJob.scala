package Preprocessing

/**
 * This class prepares views for further processing (fixes time)
 * Result is dataframe with columns:
 *   user_id - id of a user, who made this view
 *   ip - ip of a device, that was used to make this view
 *   device - type of device, that was used to make this view
 *   item_id - id of an item, that was viewed
 *   ts - time when a view was made
 */
class ViewPreprocessingJob(arguments: Map[String, String], spark: SparkSession) extends PreprocessingJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val views = data("data")
    val timeFixed = convertStringToTimeStamp("ts", views)
    repartitionByDate("ts", timeFixed)
  }
}

object ViewPreprocessingJob extends JobCompanion {
  def parser(): Parser = PreprocessingJob.parser()
}