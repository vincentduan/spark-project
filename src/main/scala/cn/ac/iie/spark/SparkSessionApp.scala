package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * SparkSession的使用
 */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    //spark.read.format("json")
    val people = spark.read.json("file:///E:/test/employees.json")
    people.show()

    spark.close()
  }
}
