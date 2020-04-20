package cn.ac.iie.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 使用Spark完成我们的数据清洗操作
 */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec", "gzip") // 指定压缩方式，默认的压缩方式是snappy
      .master("local[2]").getOrCreate()
    val acessRDD = spark.sparkContext.textFile("file:///E:/test/output/part-00000")
    // acessRDD.take(10).foreach(println)
    // RDD => DF
    val accessDF = spark.createDataFrame(acessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)
    // accessDF.printSchema()
    // accessDF.show(false)
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("file:///E:/test/clean2")
    spark.stop()
  }

}
