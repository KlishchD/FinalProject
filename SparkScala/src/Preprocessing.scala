import de.halcony.argparse.Parser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{from_unixtime, to_date, to_timestamp, udf}
import org.apache.spark.sql.{Dataset, SparkSession}

object Preprocessing {

  private val logger: Logger = Logger.getLogger("app")

  def main(args: Array[String]): Unit = {

    setUpLogging()

    val arguments = parseCLIArguments(args)

    val spark = getSpark(arguments)

    process(arguments("viewsFilepath"), arguments("repartitionedViewsFilepath"), arguments("purchasesFilepath"), arguments("repartitionedPurchasesFilepath"), spark)

    spark.close()
  }

  def setUpLogging(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /**
   * Parses cli arguments
   *
   * @param args array with cli arguments
   * @return Map[String, String] of parsed cli arguments
   */
  def parseCLIArguments(args: Array[String]): Map[String, String] = {
    val parser = Parser("Data preprocessor", "App that preprears data for processing")
      .addPositional("mode", "Mode in which app will run (dev or prod)")
      .addPositional("master", "Spark master")
      .addPositional("viewsFilepath", "Path to a file with views")
      .addPositional("purchasesFilepath", "Path to a file with purchases")
      .addPositional("repartitionedViewsFilepath", "Path to a place where to write views")
      .addPositional("repartitionedPurchasesFilepath", "Path to a place where to write purchases")
      .addOptional("serviceAccountKeyFilepath", "SAKF", description = "Path to a file wiht service account credentials")
      .addOptional("serviceAccountEmail", "SAE", description = "Email address of a service account")
    val arguments: Map[String, AnyVal] = parser.parse(args).toMap
    arguments.map(x => (x._1, String.valueOf(x._2)))
  }


  /**
   * Builds and sets up spark session for desired deployment mode
   *
   * @param args parsed cli arguments
   * @return spark session set up for desired deployment mode
   */
  def getSpark(args: Map[String, String]): SparkSession = {
    args("mode") match {
      case "dev" => getSparkForDevelopment(args("master"))
      case "prod" => getSparkForProduction(args("master"), args("serviceAccountKeyFilepath"), args("serviceAccountEmail"))
      case _ => throw new IllegalArgumentException("mode can be dev or prod")
    }
  }

  /**
   * Builds and sets up spark session for development mode
   *
   * @param master spark master
   * @return spark session set up for development mode
   */
  def getSparkForDevelopment(master: String): SparkSession = {
    SparkSession.builder().master(master).appName("Preprocessing app").getOrCreate()
  }

  /**
   * Builds and sets up spark session for the production mode
   *
   * @param master                    spark master
   * @param serviceAccountKeyFilepath filepath to the key of GCP service account
   * @param serviceAccountEmail       email of the GCP service account
   * @return spark session set up for the production mode
   */
  def getSparkForProduction(master: String, serviceAccountKeyFilepath: String, serviceAccountEmail: String): SparkSession = {
    val spark: SparkSession = SparkSession.builder().appName("Preprocessing app").master(master).getOrCreate()
    setUpServiceAccountCredentials(serviceAccountKeyFilepath, serviceAccountEmail, spark)
    spark
  }
  //TODO: add articles to words :)
  // TODO: ask about comentaries that repeate name of the function

  /**
   * Configures service account in a spark session
   *
   * @param serviceAccountKeyFilepath filepath to key of the GCP service account
   * @param serviceAccountEmail       email of the GCP srvice account
   * @param spark                     spark session to configure
   */
  def setUpServiceAccountCredentials(serviceAccountKeyFilepath: String, serviceAccountEmail: String, spark: SparkSession): Unit = {
    spark.conf.set("google.cloud.auth.service.account.enable", value = true)
    spark.conf.set("google.cloud.auth.service.account.email", serviceAccountEmail)
    spark.conf.set("google.cloud.auth.service.account.keyfile", serviceAccountKeyFilepath)

  }

  /**
   * @param viewsFilepath                  path to a csv file that contains views
   * @param repartitionedViewsFilepath     path where to write repartitioned views
   * @param purchasesFilepath              path to a json file that contains purchases
   * @param repartitionedPurchasesFilepath path where to write repartitioned purchases
   * @param spark                          spark session
   */
  def process(viewsFilepath: String, repartitionedViewsFilepath: String, purchasesFilepath: String, repartitionedPurchasesFilepath: String, spark: SparkSession): Unit = {
    processViews(viewsFilepath, repartitionedViewsFilepath, spark)
    processPurchases(purchasesFilepath, repartitionedPurchasesFilepath, spark)
  }

  def processViews(viewsFilepath: String, repartitionedViewsFilepath: String, spark: SparkSession): Unit = {
    logger.info("Started loading views")
    val views = loadViews(viewsFilepath, spark)
    logger.info("Finished loading views")

    logger.info("Started views repartitioned")
    val repartitionedViews = repartitionView(views, spark)
    logger.info("Finished views repartitioned")

    logger.info("Started writing views")
    repartitionedViews.write.parquet(repartitionedViewsFilepath)
    logger.info("Finished writing views")

  }

  def loadViews(filepath: String, spark: SparkSession): Dataset[View] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .csv(filepath)
      .select($"user_id", $"device", $"ip", $"item_id", to_timestamp($"ts").as("ts"))
      .as[View]
  }

  def repartitionView(views: Dataset[View], spark: SparkSession): Dataset[View] = {
    import spark.implicits._
    views.repartitionByRange(to_date($"ts"))
  }

  def processPurchases(purchasesFilepath: String, repartitionedPurchasesFilepath: String, spark: SparkSession): Unit = {
    logger.info("Started loading purchases")
    val purchases = loadPurchases(purchasesFilepath, spark)
    logger.info("Finished loading purchases")

    logger.info("Started purchases repartitioned")
    val repartitionedPurchases = repartitionPurchases(purchases, spark)
    logger.info("Finished purchases repartitioned")

    logger.info("Started writing purchases")
    repartitionedPurchases.write.parquet(repartitionedPurchasesFilepath)
    logger.info("Finished writing purchases")

  }

  def loadPurchases(filepath: String, spark: SparkSession): Dataset[Purchase] = {
    import spark.implicits._
    val parse = udf((values: Array[Array[String]]) => for (value <- values) yield (value(0), value(1).toInt))
    spark.read
      .json(filepath)
      .select($"order_id", $"user_id", $"ip",
        parse($"items").as("items"), to_timestamp(from_unixtime($"ts" / 1000, "yyyy-MM-dd HH:mm:ss.SSSS")).as("ts"))
      .as[Purchase]
  }

  def repartitionPurchases(purchases: Dataset[Purchase], spark: SparkSession): Dataset[Purchase] = {
    import spark.implicits._
    purchases.repartitionByRange(to_date($"ts"))
  }

  case class View(item_id: String, user_id: String, ip: String, ts: Timestamp)

  case class Purchase(order_id: String, user_id: String, ip: String, items: Array[(String, Int)], ts: Timestamp)
}
