package Utils

import org.apache.spark.sql.DataFrame

object Writing {
  def writeToParquet(data: DataFrame, filepath: String): Unit = {
    data.write.parquet(filepath)
  }
}
