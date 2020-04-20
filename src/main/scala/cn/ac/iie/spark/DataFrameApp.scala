package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * DataFrame API基本操作
 */

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    // 本地文件系统或者HDFS都支持
    // 将Json文件加载成一个DataFrame
    val peopleDF = spark.read.format("json").load("file:///E:/test/employees.json")
    // 输出DataFrame对应的Schema信息
    peopleDF.printSchema()
    // 默认展示数据集前20条记录
    peopleDF.show()
    //查询某列的所有数据,相当于mysql中的 select name from
    peopleDF.select("name").show()
    peopleDF.select(peopleDF.col("name"), peopleDF.col("age") + 10).show()
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    // 根据某一列的值进行过滤： select * from table where age > 20
    peopleDF.filter(peopleDF.col("age") > 20).show()

    // 根据某一列进行分组，然后在进行聚合操作:select age, count(1) from table group by age
    peopleDF.groupBy(peopleDF.col("age")).count().show()
    spark.stop()
  }
}
