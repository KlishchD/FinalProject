import Aggregations.Purchases.LocationsSellNumberShareAggregationJob
import Builder.{getIps, getPurchasesWithOutRepetitionOfOneUser, getSpark}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class LocationsSellNumberShareAggregationJobTest extends AnyFunSuite {
  private lazy val spark = getSpark()

  test("Check process") {
    val expected = Set(
      Row("Ukraine", 0.5),
      Row("United States", 0.5))

    val job = new LocationsSellNumberShareAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "purchases" -> getPurchasesWithOutRepetitionOfOneUser(spark),
      "ips" -> getIps(spark),
    )

    val result = job.process(data)

    assert(check(expected, result))
  }
}
