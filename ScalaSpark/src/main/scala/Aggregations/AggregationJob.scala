package Aggregations

import JobManagment.{Job, JobCompanion}
import Utils.ArgumentsParsing.RichParser
import Utils.Parsing.addColumnWithCurrentTime
import Utils.Writing.{writeToBigQuery, writeToPostgres}
import de.halcony.argparse.Parser
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class AggregationJob(arguments: Map[String, String], spark: SparkSession) extends Job(arguments, spark) {
  override def write(data: DataFrame): Unit = {
    val dataWithWriteTimeStamp = addColumnWithCurrentTime("aggregation_timestamp", data)

    arguments("mode") match {
      case "dev" => writeToPostgres(arguments("postgresUrl"), arguments("resultTable"), arguments("postgresUser"), arguments("postgresPassword"), dataWithWriteTimeStamp)
      case "prod" => writeToBigQuery(arguments("temporaryBucketName"), arguments("resultTable"), arguments("bigQueryServiceAccountKeyFilepath"), dataWithWriteTimeStamp)
    }
  }
}


object AggregationJob extends JobCompanion {
  override def parser(): Parser = {
    Job.parser()
      .addMode()
      .addPostgres()
      .addBigQuery()
      .addResultTable()
  }
}