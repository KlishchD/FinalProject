import Aggregations.Purchases.{ConversionRateAggregationJob, GroupedItemsAggregationJob, LocationsSellNumberShareAggregationJob, ProfitByItemAggregationJob}
import Aggregations.Views.{ItemViewsShareAggregationJob, ItemsViewsCountAggregationJob}
import JobManagment.JobRunner

object RunAggregations extends App {

  val runner = new JobRunner


  runner.register("conversionRate", classOf[ConversionRateAggregationJob], ConversionRateAggregationJob.parser())
  runner.register("groupedItems", classOf[GroupedItemsAggregationJob], GroupedItemsAggregationJob.parser())
  runner.register("locationsShare", classOf[LocationsSellNumberShareAggregationJob], LocationsSellNumberShareAggregationJob.parser())
  runner.register("profitByItem", classOf[ProfitByItemAggregationJob], ProfitByItemAggregationJob.parser())
  runner.register("itemViews", classOf[ItemViewsShareAggregationJob], ItemViewsShareAggregationJob.parser())
  runner.register("itemsViewsCount", classOf[ItemsViewsCountAggregationJob], ItemsViewsCountAggregationJob.parser())

  val jobName = args(0)
  val sparkMaster = args(1)
  val sparkAppName = args(2)
  val arguments = args.drop(3)

  runner.run(jobName, sparkMaster, sparkAppName, arguments)
}
