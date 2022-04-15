import sys

from Aggregations.Purchases.GroupedItemsAggregationJob import GroupedItemsAggregationJob
from Aggregations.Purchases.LocationSellNumberShareAggregationJob import LocationSellNumberShareAggregation
from Aggregations.Purchases.ProfitByItemAggregationJob import ProfitByItemAggregationJob
from Aggregations.Views.ConversionRateAggregationJob import ConversionRateAggregationJob
from Aggregations.Views.ItemViewsShareAggregationJob import ItemViewsShareAggregationJob
from Aggregations.Views.ItemsViewsCountAggregationJob import ItemsViewsCountAggregationJob
from JobManagment.JobRunner import JobRunner


def __main__():
    runner = JobRunner()

    runner.register("groupedItems", GroupedItemsAggregationJob)
    runner.register("locationSellShare", LocationSellNumberShareAggregation)
    runner.register("profitByItem", ProfitByItemAggregationJob)
    runner.register("conversionRate", ConversionRateAggregationJob)
    runner.register("itemsViewsCount", ItemsViewsCountAggregationJob)
    runner.register("itemsViewsShare", ItemViewsShareAggregationJob)

    job_name = sys.argv[1]
    spark_master = sys.argv[2]
    spark_app_name = sys.argv[3]
    arguments = sys.argv[4:]

    runner.run(job_name, spark_master, spark_app_name, arguments)


if __name__ == "__main__":
    __main__()
