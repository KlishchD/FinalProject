package Utils

import de.halcony.argparse.Parser

object CLIArgumentsParsing {

  def parseArguments(argument: Array[String], parser: Parser): Map[String, String] = {
    parser.parse(argument).toMap.map(retrieve)
  }

  def retrieve(x: (String, Any)): (String, String) = {
    if (x._2.isInstanceOf[Some[_]]) (x._1, x._2.asInstanceOf[Some[_]].get.toString)
    else (x._1, x._2.toString)
  }

  implicit class EnrichedParser(parse: Parser) {
    def addMode(): Parser = {
      parse.addOptional("mode", "m", description = "Mode in which app will run (dev or prod)")
    }

    def addDynamicTableDataSource(name: String): Parser = {
      parse.addOptional(f"${name}Filepath", f"${name}fp", description = f"Path to a file with $name")
    }

    def addStaticTableDataSource(name: String): Parser = {
      parse.addOptional(f"${name}filepath", f"${name}fp", description = f"Filepath to a file with $name")
      parse.addOptional(f"${name}keyPattern", f"${name}kp", description = f"Key pattern of $name")
      parse.addOptional(f"${name}keyColumn", f"${name}kc", description = f"Key column of $name")
    }

    def addFormatForData(name: String): Parser = {
      parse.addOptional(f"${name}Format", f"${name}f", description = f"Format of ${name}")
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

  }

}
