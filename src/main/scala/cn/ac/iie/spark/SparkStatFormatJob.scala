package cn.ac.iie.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

/**
 * 第一步：抽取出我们所需要的指定列的数据
 */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val access = spark.sparkContext.textFile("file:///E:/test/SD_010563_0")
    // access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split("\t")
      val uid = splits(0)
      val time = DateUtils.parse(splits(1))
      val desc_info = splits(2)
      val related_identity = splits(3)
      val jsonObject = JSON.parseObject(related_identity)
      val ip = jsonObject.getString("ip")
      val num = splits(4)
      val desc_type = splits(5)
      val date = splits(6)
//      (uid, time, desc_info, related_identity, desc_type, date)
      uid + "\t" + time +"\t" + desc_info +"\t" + ip +  "\t" + desc_type + "\t" +date+ "\t" +num
    //}).take(20).foreach(println)
    }).saveAsTextFile("file:///E:/test/output")

    spark.close()
  }
}
