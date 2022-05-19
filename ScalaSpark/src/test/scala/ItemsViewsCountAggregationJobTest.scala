import Aggregations.Views.ItemsViewsCountAggregationJob
import Builder.{getIps, getSpark, getViews}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class ItemsViewsCountAggregationJobTest extends AnyFunSuite {
  private lazy val spark = getSpark()

  test("Check process") {
    val expected = Set(
      Row("item22573068", 1),
      Row("item33975825", 1),
      Row("item48000248", 1),
      Row("item52013010", 1),
      Row("item68379072", 1))

    val job = new ItemsViewsCountAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "views" -> getViews(spark),
      "ips" -> getIps(spark)
    )

    val result = job.process(data)

    assert(check(expected, result))
  }
}
