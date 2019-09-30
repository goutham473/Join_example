package model

import org.apache.spark.sql.DataFrame

object Writer {

  def csvWriter(inDf: DataFrame, outputPath: String) = inDf.write.mode("overwrite").save(outputPath)

}
