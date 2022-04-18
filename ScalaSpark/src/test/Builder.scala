import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.Utils.Parsing.convertStringToTimeStamp

object Builder {

  def getViews(spark: SparkSession): DataFrame = {
    import spark.implicits._
    convertStringToTimeStamp("ts", Seq(
      ("user6854276", "Phone", "25.63.215.24", "item22573068", "2022-04-13 12:58:16.065862"),
      ("user4118700", "Phone", "236.27.45.213", "item33975825", "2022-04-13 11:12:30.067085"),
      ("user5340766", "Phone", "86.225.224.38", "item48000248", "2022-04-13 11:32:54.067100"),
      ("user3053687", "Tablet", "223.8.154.63", "item52013010", "2022-04-13 11:41:28.067108"),
      ("user6854276", "Phone", "25.63.215.24", "item68379072", "2022-04-13 11:21:41.067116")
    ).toDF("user_id", "device", "ip", "item_id", "ts"))
  }

  def getPurchasesWithOutRepetitionOfOneUser(spark: SparkSession): DataFrame = {
    import spark.implicits._
    convertStringToTimeStamp("ts", Seq(
      ("user6854276", "25.63.215.24", "item22573068", "2022-04-13 12:58:16.065862", 3),
      ("user5340766", "86.225.224.38", "item48000248", "2022-04-13 11:32:54.067100", 1)
    ).toDF("user_id", "ip", "item_id", "ts", "amount"))
  }

  def getPurchasesWithRepetitionOfOneUser(spark: SparkSession): DataFrame = {
    import spark.implicits._
    convertStringToTimeStamp("ts", Seq(
      ("user6854276", "25.63.215.24", "item22573068", "2022-04-13 12:58:16.065862", 3),
      ("user5340766", "86.225.224.38", "item48000248", "2022-04-13 11:32:54.067100", 1),
      ("user6854276", "25.63.215.24", "item68379072", "2022-04-13 12:58:16.065862", 3)
    ).toDF("user_id", "ip", "item_id", "ts", "amount"))
  }

  def getIps(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("25.63.215.24", "Ukraine"),
      ("236.27.45.213", "Ukraine"),
      ("86.225.224.38", "United States"),
      ("223.8.154.63", "United Kingdom"),
    ).toDF("ip", "country")
  }

  def getUser(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("user6854276", "Phone", "25.63.215.24"),
      ("user4118700", "Phone", "236.27.45.213"),
      ("user5340766", "Phone", "86.225.224.38"),
      ("user3053687", "Tablet", "223.8.154.63"),
      ("user6854276", "Phone", "25.63.215.24")
    ).toDF("user_id", "device", "ip")
  }

  def getItems(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("item22573068", 12),
      ("item33975825", 3),
      ("item48000248", 14),
      ("item52013010", 2),
      ("item68379072", 5)
    ).toDF("item_id", "price")
  }

  def getSpark(): SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    SparkSession.builder().master("local[*]").appName("App").getOrCreate()
  }

}
