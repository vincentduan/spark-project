package cn.ac.iie.spark

import org.apache.spark.sql.SparkSession

/**
 * 使用外部数据源综合查询Hive和Mysql的表数据
 */
object HiveMysqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMysqlApp").master("local[2]").getOrCreate()
    // 加载hive表数据
    val hiveDF = spark.table("emp")
    // 加载mysql数据
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://swarm-manager:3306")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "spark.DEPT")
      .option("user", "root")
      .option("password", "123456")
      .load()
    // JOIN
    val resultDF = hiveDF.join(mysqlDF,hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),mysqlDF.col("deptno"),mysqlDF.col("dname")).show()
    resultDF.show()
    spark.close()
  }
}
