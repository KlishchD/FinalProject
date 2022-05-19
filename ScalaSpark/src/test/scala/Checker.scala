import org.apache.spark.sql.{DataFrame, Row}

object Checker {

  def check(expected: Set[Row], data: DataFrame): Boolean = {
    val convertedResul = data.collect().toSet
    expected.equals(convertedResul)
  }

}
