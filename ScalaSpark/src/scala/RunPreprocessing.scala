import JobManagment.JobRunner
import Preprocessing.{PurchasesPreprocessingJob, ViewPreprocessingJob}

object RunPreprocessing extends App {

  val runner = new JobRunner

  runner.register("views", classOf[ViewPreprocessingJob], ViewPreprocessingJob.parser())
  runner.register("purchases", classOf[PurchasesPreprocessingJob], PurchasesPreprocessingJob.parser())

  val jobName = args(0)
  val sparkMaster = args(1)
  val sparkAppName = args(2)
  val arguments = args.drop(3)

  runner.run(jobName, sparkMaster, sparkAppName, arguments)
}
