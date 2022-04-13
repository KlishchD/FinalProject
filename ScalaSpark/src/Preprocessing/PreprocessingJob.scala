package Preprocessing

import JobManagment.Job
import Utils.CLIArgumentsParsing.EnrichedParser
import Utils.Loading.loadFromFile
import Utils.Services.setUpServiceAccount
import Utils.Writing.writeToParquet
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class PreprocessingJob(arguments: Map[String, String], spark: SparkSession) extends Job(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    if (arguments("mode") == "prod")
      setUpServiceAccount("readServiceAccountEmail", "readServiceAccountKeyFilepath", spark)

    Map("data" -> loadFromFile(arguments("filepath"), arguments("format"), spark))
  }

  override def write(data: DataFrame): Unit = {
    if (arguments("mode") == "prod")
      setUpServiceAccount(arguments("writeServiceAccountEmail"), arguments("writeServiceAccountKeyFilepath"), spark)
    writeToParquet(data, arguments("resultsFilepath"))
  }
}

object PreprocessingJob {
  def parser(): Parser = {
    Parser("Data preprocessor")
      .addMode()
      .addDynamicTableDataSource("data")
      .addFormatForData("data")
      .addReadServiceAccount()
      .addWriteServiceAccount()
      .addResultFilepath()
  }
}