package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * Schema Info
 */
object SchemaInferApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()
    val df = spark.read.format("json").load("file:///E:/test/sd_data_test.json")
    df.printSchema()
    df.show(false)
    spark.close()
  }
}
