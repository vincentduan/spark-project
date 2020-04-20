package cn.ac.iie.BigDataCenter

import org.apache.spark.sql.{SaveMode, SparkSession}

object TwitterClean {
  def main(args: Array[String]): Unit = {
    val sparksession = SparkSession.builder().appName("TwitterClean").master("local[2]").getOrCreate()
    val df = sparksession.read.format("json").load("file:///C:\\Users\\vincent\\Documents\\WeChat Files\\duan_vincent\\FileStorage\\File\\2020-04\\数据样例\\facebook_status.txt")
    df.printSchema()
    sparksession.stop()
  }
}
