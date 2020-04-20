package cn.ac.iie.log

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
 * 访问日志转换工具类
 */
object AccessConvertUtil {
  // 定义输出的字段
  val struct = StructType(
    Array(
      StructField("uid", StringType),
      StructField("time", StringType),
      StructField("netType", StringType),
      StructField("ip", StringType),
      StructField("desc_type", StringType),
      StructField("day", StringType),
      StructField("num", LongType),
      StructField("city", StringType)

    )
  )

  /**
   * 根据输入的每一行信息转换成输出的样式
   * @param log 输入的每一行记录信息
   */
  def parseLog(log:String) = {
    try{
      val splits = log.split("\t")
      val id = splits(0)
      val desc_type = splits(4)
      val num = splits(6).toLong
      val ip = splits(3)
      val desc_info = splits(2)
      val desc_info_json = JSON.parseObject(desc_info)
      var netType = ""
      if(desc_info_json.containsKey("nettype")){
        netType = desc_info_json.getString("nettype")
      }
      val city = IpUtils.getCity(ip)
      val time = splits(1)
      val day = splits(5)


      // 这个Row里面的字段要和struct中的字段对应上
      Row(id, time, netType, ip, desc_type, day,num, city)
    }catch {
      case e: Exception => Row(0)
    }

  }
}
