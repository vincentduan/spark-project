package cn.ac.iie.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * TopN 统计spark作业
 */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()
    val accessDF = spark.read.format("parquet").load("file:///E:/test/clean")
    // accessDF.printSchema()
    // accessDF.show(false)
    val day = "20190702"
    StatDao.deleteDay(day)
    // 最受欢迎的TopN netType
     netTypeAccessTopNStat(spark, accessDF, day)
    // 按照地市进行统计TopN课程
     cityTypeAccessTopNStat(spark, accessDF,day)
    // 按照流量进行统计
     netTypeTrafficTopNStat(spark, accessDF, day)
    spark.stop
  }

  /**
   * 按流量进行统计
   * @param spark
   * @param accessDF
   */
  def netTypeTrafficTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    val trafficsTopNDF = accessDF.filter(accessDF.col("day") === day && accessDF.col("netType") === "wifi")
      .groupBy("day", "uid").agg(sum("num").as("traffics"))
      .orderBy(desc("traffics"))
      // .show(false)

    // 将统计结果写入到Mysql中
    try {
      trafficsTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayNetTypeTrafficsStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val uid = info.getAs[String]("uid").toLong
          val traffics = info.getAs[Long]("traffics")
          list.append(DayNetTypeTrafficsStat(day, uid, traffics))
        })
        StatDao.insertDayNetTypeTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

    /**
   * 按照地市进行统计Top3课程
   *
   * @param spark
   * @param accessDF
   */
  def cityTypeAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    val cityAccessTopNDF = accessDF.filter(accessDF.col("day") === day && accessDF.col("netType") === "wifi")
      .groupBy("day", "uid", "city").agg(count("uid").as("times")).orderBy(desc("times"))
    cityAccessTopNDF.show(false)
    // window 函数在Spark SQL的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day")
      , cityAccessTopNDF("uid")
      , cityAccessTopNDF("city")
      , cityAccessTopNDF("times")
      , row_number()
        .over(Window.partitionBy("city")
          .orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")
    ).filter("times_rank <= 3")
      //.show(false)

    // 将统计结果写入到Mysql中
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityNetTypeAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val uid = info.getAs[String]("uid").toLong
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityNetTypeAccessStat(day, uid, city, times, timesRank))
        })
        StatDao.insertDayNetTypeCityAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * 最受欢迎的TopN netType
   *
   * @param spark
   * @param accessDF
   */
  def netTypeAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
        val wifiAccessTopNDF = accessDF.filter(accessDF.col("day") === day && accessDF.col("netType") === "wifi")
          .groupBy("day", "uid").agg(count("uid").as("times")).orderBy(desc("times"))
        wifiAccessTopNDF.show(false)

//    accessDF.createOrReplaceTempView("access_logs")
//    val wifiAccessTopNDF = spark.sql("select day,uid,count(1) as times from access_logs where day='20190702' and netType='wifi' group by day,uid order by times desc")
//    wifiAccessTopNDF.show(false)

    // 将统计结果写入到Mysql中
    try {
      wifiAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayNetTypeAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val uid = info.getAs[String]("uid").toLong
          val times = info.getAs[Long]("times")
          list.append(DayNetTypeAccessStat(day, uid, times))
        })
        StatDao.insertNetTypeAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }
}
