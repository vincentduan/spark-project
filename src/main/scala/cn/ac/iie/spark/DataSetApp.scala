package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * DataSet 操作
 */
object DataSetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    // spark如何解析csv文件
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///E:/test/infos.txt")
    df.show()
    // 注意需要导入隐式转换
    import spark.implicits._

    val ds = df.as[Infos]
    ds.map(line => line.id).show()
    spark.close()
  }

  case class Infos(id: Int, name: String, age: Int)

}
