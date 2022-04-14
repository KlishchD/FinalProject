package Preprocessing

import JobManagment.JobCompanion
import Utils.Parsing.{convertUnixToTimeStamp, repartitionByDate}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

class PurchasesPreprocessingJob(arguments: Map[String, String], spark: SparkSession) extends PreprocessingJob(arguments, spark) {
  override def process(data: Map[String, DataFrame]): DataFrame = {
    val purchases = data("data")
    val timeFixed = convertUnixToTimeStamp("ts", purchases)
    repartitionByDate("ts", timeFixed)
  }
}

object PurchasesPreprocessingJob extends JobCompanion {
  def parser(): Parser = PreprocessingJob.parser()
}
