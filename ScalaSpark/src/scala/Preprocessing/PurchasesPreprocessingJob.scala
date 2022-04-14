package Preprocessing

import JobManagment.JobCompanion
import Utils.Parsing.{convertUnixToTimeStamp, explodeDataFrameColumn, extractArrayElementToNewColumn, repartitionByDate}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

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
