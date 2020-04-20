package cn.ac.iie.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * DataFrame和RDD的互操作
 */
object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkSessionApp = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    // infoReflection(sparkSessionApp)
    program(sparkSessionApp)
    sparkSessionApp.close()
  }

  private def program(sparkSessionApp: SparkSession) = {
    val rdd = sparkSessionApp.sparkContext.textFile("file:///E:/test/infos.txt")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id", IntegerType, true),StructField("name", StringType, true),StructField("age", IntegerType, true)))
    val infoDF = sparkSessionApp.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()
  }

  private def infoReflection(sparkSessionApp: SparkSession) = {
    // 将RDD转成DataFrame
    val rdd = sparkSessionApp.sparkContext.textFile("file:///E:/test/infos.txt")

    // 注意需要导入隐式转换
    import sparkSessionApp.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 25).show()
    infoDF.createOrReplaceTempView("infos")
    sparkSessionApp.sql("select * from infos where age > 25").show()
  }

  case class Info(id:Int, name:String, age:Int){

  }
}
