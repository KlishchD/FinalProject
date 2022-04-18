package Preprocessing

import JobManagment.{Job, JobCompanion}
import Utils.ArgumentsParsing.RichParser
import Utils.Loading.loadFromFile
import Utils.Services.setUpServiceAccount
import Utils.Writing.writeToParquet
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class PreprocessingJob(arguments: Map[String, String], spark: SparkSession) extends Job(arguments, spark) {
  override def load(): Map[String, DataFrame] = {
    if (arguments("mode") == "prod")
      setUpServiceAccount("readServiceAccountEmail", "readServiceAccountKeyFilepath", spark)

    Map("data" -> loadFromFile(arguments("datafp"), arguments("dataf"), spark))
  }

  override def write(data: DataFrame): Unit = {
    if (arguments("mode") == "prod")
      setUpServiceAccount(arguments("writeServiceAccountEmail"), arguments("writeServiceAccountKeyFilepath"), spark)
    writeToParquet(data, arguments("resultsFilepath"))
  }
}

object PreprocessingJob extends JobCompanion {
  def parser(): Parser = {
    Job.parser()
      .addMode()
      .addDynamicTableDataSource("data")
      .addFormatForData("data")
      .addReadServiceAccount()
      .addWriteServiceAccount()
      .addResultFilepath()
  }
}