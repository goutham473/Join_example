package model

import org.apache.spark.sql._

object Reader {

  def textReader(spark: SparkSession, inputPath: String) = spark.read.textFile(inputPath)

  def csvReader(spark: SparkSession, inputPath: String) = spark.read.option("header", "true")csv(inputPath)

}
