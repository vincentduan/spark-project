package cn.ac.iie.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * HiveContext的使用
 * 使用时需要通过 --jars 把mysql的驱动传递到classpath
 */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 1. 创建相应的Context
    val sparkConf= new SparkConf()
    // 在生产或测试环境中，APPName和Master是通过脚本指定的
    sparkConf.setAppName("HiveContextApp")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    // 2. 相关处理:json
    hiveContext.table("sal").show()
    // 3. 关闭资源
    sc.stop()
  }
}
