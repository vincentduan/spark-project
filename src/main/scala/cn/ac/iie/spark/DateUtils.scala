package cn.ac.iie.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间解析工具类
 * SimpleDateFormat 是线程不安全的
 */
object DateUtils {
  // 输入文件日期格式
  val YYYYMMDDHHMM_TIME_FORMAT =  FastDateFormat.getInstance("yyyyMMddHHmmss", Locale.ENGLISH)
  // 目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 获取时间yyyy-MM-dd HH:mm:ss
   * @param time
   */
  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
   * 获取输入日志时间：long类型
   * @param time
   * @return
   */
  def getTime(time: String) = {
    try{
      YYYYMMDDHHMM_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception => 0L
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("20190719210330"))
  }

}
