package cn.ac.iie.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 使用Spark完成我们的数据清洗操作, 运行在Yarn之上
 */
object SparkStatCleanJobYARN {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("Usage: SparkStatCleanJobYARN <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args
    val spark = SparkSession.builder().getOrCreate()
    val acessRDD = spark.sparkContext.textFile(inputPath)

    // acessRDD.take(10).foreach(println)

    // RDD => DF
    val accessDF = spark.createDataFrame(acessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)
    // accessDF.printSchema()
    // accessDF.show(false)
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)
    spark.stop()
  }

}
