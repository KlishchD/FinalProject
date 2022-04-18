import Aggregations.Purchases.ProfitByItemAggregationJob
import Builder.{getIps, getItems, getPurchasesWithOutRepetitionOfOneUser, getSpark}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class ProfitByItemAggregationJobTest extends AnyFunSuite {
  private lazy val spark = getSpark()

  test("Check process") {
    val expected = Set(
      Row("item22573068", 36),
      Row("item48000248", 14))

    val job = new ProfitByItemAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "purchases" -> getPurchasesWithOutRepetitionOfOneUser(spark),
      "ips" -> getIps(spark),
      "items" -> getItems(spark)
    )

    val result = job.process(data)

    assert(check(expected, result))
  }

}
