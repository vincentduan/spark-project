package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * Parquet文件操作
 */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val sparksession = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()
    val df = sparksession.read.format("parquet").load("file:///E:/test/users.parquet")
    df.printSchema()
    df.show()
    df.select("name", "favorite_numbers").write.format("json").save("file:///E:/test/users-result")
    sparksession.sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    sparksession.read.format("parquet").option("path", "file:///E:/test/users.parquet").load()
    sparksession.close()
  }
}
