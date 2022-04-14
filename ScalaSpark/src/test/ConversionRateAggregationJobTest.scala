import Aggregations.Purchases.ConversionRateAggregationJob
import Builder.{getIps, getPurchasesWithOutRepetitionOfOneUser, getSpark, getViews}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ConversionRateAggregationJobTest extends AnyFunSuite with BeforeAndAfter {
  private lazy val spark = getSpark()

  test("Check process") {
    val expected = Set(
      Row("item22573068", 1.0),
      Row("item33975825", 0.0),
      Row("item48000248", 1.0),
      Row("item52013010", 0.0),
      Row("item68379072", 0.0))

    val job = new ConversionRateAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "purchases" -> getPurchasesWithOutRepetitionOfOneUser(spark),
      "ips" -> getIps(spark),
      "views" -> getViews(spark)
    )

    val result = job.process(data)

    assert(check(expected, result))
  }


}
