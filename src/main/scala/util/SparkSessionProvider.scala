package util

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  implicit val sparkSession = SparkSession.builder().appName("Joins").getOrCreate()

}
