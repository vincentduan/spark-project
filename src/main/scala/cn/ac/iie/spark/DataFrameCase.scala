package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * DataFrame中的操作
 */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val DataFrameCase = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()
    val rdd = DataFrameCase.sparkContext.textFile("file:///E:/test/student.data")

    // 注意需要导入隐式转换
    import DataFrameCase.implicits._
    val studenntDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studenntDF.show()
    studenntDF.head(5)
    studenntDF.take(10)
    studenntDF.select("email").show(20, false)
    studenntDF.filter("name='' OR name='NULL'").show(false)
    studenntDF.filter("SUBSTR(name,0,1)='v'").show(false)
    studenntDF.sort(studenntDF.col("name")).show()
    studenntDF.sort(studenntDF.col("name").desc).show()
    studenntDF.sort(studenntDF.col("name").desc, studenntDF.col("email")).show()

    val studenntDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studenntDF.join(studenntDF2, studenntDF.col("id") === studenntDF2.col("id"), "inner").show()

    DataFrameCase.close()
  }
  case class Student(id: Int, name: String, phone: String, email: String)
}
