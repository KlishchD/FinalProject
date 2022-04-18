package JobManagment


import de.halcony.argparse.Parser

trait Runner {

  def register[T <: Job](jobName: String, jobClass: Class[T], argumentsParser: Parser): Unit

  def run(jobName: String, sparkMaster: String, sparkAppName: String, args: Array[String]): Unit

}
