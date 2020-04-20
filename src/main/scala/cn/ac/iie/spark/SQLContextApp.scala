package cn.ac.iie.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * SQLContextApp 的使用
 * 注意：IDEA是在本地，而测试数据在服务器上，能不能在本地进行开发测试？可以
 */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val path = args(0)

    // 1. 创建相应的Context
    val sparkConf= new SparkConf()
    // 在生产或测试环境中，APPName和Master是通过脚本指定的
    //sparkConf.setAppName("SQLContextApp")
    //sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 2. 相关处理:json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
    // 3. 关闭资源
    sc.stop()
  }
}
