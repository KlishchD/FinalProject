import Aggregations.Purchases.GroupedItemsAggregationJob
import Builder.{getIps, getPurchasesWithOutRepetitionOfOneUser, getPurchasesWithRepetitionOfOneUser, getSpark}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class GroupedItemsAggregationJobTest extends AnyFunSuite {
  private lazy val spark = getSpark()

  test("Check process, input has no two item bought by one user. Thus result should be empty DataFrame") {
    val expected = Set[Row]()

    val job = new GroupedItemsAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "purchases" -> getPurchasesWithOutRepetitionOfOneUser(spark),
      "ips" -> getIps(spark),
    )

    val result = job.process(data)

    assert(check(expected, result))
  }

  test("Check process, input has only one user that bought two different items. Thus result should be DataFrame with") {
    val expected = Set(Row("item68379072", "item22573068", 1))

    val job = new GroupedItemsAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "purchases" -> getPurchasesWithRepetitionOfOneUser(spark),
      "ips" -> getIps(spark),
    )

    val result = job.process(data)

    assert(check(expected, result))

  }
}
