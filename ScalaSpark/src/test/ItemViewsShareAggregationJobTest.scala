import Aggregations.Views.ItemViewsShareAggregationJob
import Builder.{getIps, getSpark, getViews}
import Checker.check
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class ItemViewsShareAggregationJobTest extends AnyFunSuite {
  private lazy val spark = getSpark()

  test("Check process") {
    val expected = Set(
      Row("item22573068", 0.2),
      Row("item33975825", 0.2),
      Row("item48000248", 0.2),
      Row("item52013010", 0.2),
      Row("item68379072", 0.2))

    val job = new ItemViewsShareAggregationJob(Map("timeFrame" -> "None", "devices" -> "None", "locations" -> "None"), spark)

    val data = Map(
      "views" -> getViews(spark),
      "ips" -> getIps(spark)
    )

    val result = job.process(data)

    assert(check(expected, result))
  }

}
