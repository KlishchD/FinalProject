package Preprocessing


import JobManagment.JobCompanion
import Utils.Parsing.{convertStringToTimeStamp, repartitionByDate}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

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