package JobManagment

import Utils.ArgumentsParsing.parseArguments
import de.halcony.argparse.Parser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class JobRunner extends Runner {
  private var registeredJobs: Map[String, (Class[Job], Parser)] = Map()

  override def register[T <: Job](jobName: String, jobClass: Class[T], argumentsParser: Parser): Unit = {
    registeredJobs = registeredJobs + (jobName -> (jobClass.asInstanceOf[Class[Job]], argumentsParser))
  }

  override def run(jobName: String, sparkMaster: String, sparkAppName: String, args: Array[String]): Unit = {
    val jobTuple = registeredJobs(jobName)

    val parser = jobTuple._2

    val arguments = parseArguments(args, parser)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()

    val job = getJobInstance(arguments, spark, jobTuple._1)

    val data = job.load()

    val result = job.process(data)

    job.write(result)
  }

  private def getJobInstance[T](arguments: Map[String, String], spark: SparkSession, clazz: Class[T]): T = {
    val constructors = clazz.getConstructors

    val constructor = constructors(0)

    constructor.newInstance(arguments, spark).asInstanceOf[T]
  }
}
