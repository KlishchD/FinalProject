package Utils

import com.github.nscala_time.time.Imports.{richDateTime, richInt}
import de.halcony.argparse.Parser
import org.joda.time.DateTime

object ArgumentsParsing {

  def parseArguments(argument: Array[String], parser: Parser): Map[String, String] = {
    parser.parse(argument).toMap.map(retrieve)
  }

  def retrieve(x: (String, Any)): (String, String) = {
    if (x._2.isInstanceOf[Some[_]]) (x._1, x._2.asInstanceOf[Some[_]].get.toString)
    else (x._1, x._2.toString)
  }

  def parseTimeFrame(time: String): (DateTime, DateTime) = {
    val now = DateTime.now()
    time match {
      case "day" =>
        (now - 1.day, now)
      case "week" =>
        (now - 7.days, now)
      case "month" =>
        (now - 1.month, now)
      case "year" =>
        (now - 1.year, now)
    }
  }

  def parseArray(str: String): Array[String] = {
    str.split(",")
  }

  implicit class RichParser(parse: Parser) {
    def addMode(): Parser = {
      parse.addOptional("mode", "m", description = "Mode in which app will run (dev or prod)")
    }

    def addDynamicTableDataSource(name: String): Parser = {
      parse.addOptional(f"${name}Filepath", f"${name}fp", description = f"Path to a file with $name")
    }

    def addStaticTableDataSource(name: String): Parser = {
      parse.addOptional(f"${name}Filepath", f"${name}fp", description = f"Filepath to a file with $name")
      parse.addOptional(f"${name}KeyPattern", f"${name}kp", description = f"Key pattern of $name")
      parse.addOptional(f"${name}KeyColumn", f"${name}kc", description = f"Key column of $name")
    }

    def addFormatForData(name: String): Parser = {
      parse.addOptional(f"${name}Format", f"${name}f", description = f"Format of $name")
    }

    def addReadServiceAccount(): Parser = {
      parse
        .addOptional("readServiceAccountKeyFilepath", "SAKF", description = "Path to a file with service account credentials for reading from GCS")
        .addOptional("readServiceAccountEmail", "SAE", description = "Email address of a service account for reading from GCS")
    }

    def addWriteServiceAccount(): Parser = {
      parse
        .addOptional("writeServiceAccountKeyFilepath", "SAKF", description = "Path to a file with service account credentials for writing to GCS")
        .addOptional("writeServiceAccountEmail", "SAE", description = "Email address of a service account for writing to a GCS")
    }

    def addResultFilepath(): Parser = {
      parse.addOptional("resultsFilepath", "rfp", description = "Path to a place where to write results")
    }

    def addLocation(): Parser = {
      parse.addOptional("locations", "l", description = "List of locations to include in aggregation (omit if you want to get aggregation for all locations")
    }

    def addTimeFrame(): Parser = {
      parse.addOptional("timeFrame", "t", description = "Time to include in aggregation (omit if you want to get aggregation all time)")
    }

    def addDevices(): Parser = {
      parse.addOptional("devices", "d", description = "List of devices to include in aggregation (omit if want to get aggregation for all devices)")
    }

    def addPostgres(): Parser = {
      parse
        .addOptional("postgresUrl", "ph", description = "Jdbc url of postgres")
        .addOptional("postgresUser", "pu", description = "Postgres user")
        .addOptional("postgresPassword", "pp", description = "Postgres user password")
    }

    def addRedis(): Parser = {
      parse
        .addOptional("redisHost", "rh", description = "Host of redis")
        .addOptional("redisPort", "rp", description = "Port of redis")
    }

    def addBigQuery(): Parser = {
      parse
        .addOptional("bigQueryServiceAccountKeyFilepath", "bqsakfp", description = "Path to a file with service account credential to write in BigQuery")
        .addOptional("temporaryBucketName", "tbn", description = "Name of temporary bucket for writing to big query")
    }

    def addResultTable(): Parser = {
      parse.addOptional("resultTable", "rt", description = "Table to write results in")
    }
  }

}
