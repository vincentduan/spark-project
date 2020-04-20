package cn.ac.iie.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
 * 各个维度统计的DAO操作
 */
object StatDao {

  /**
   * 批量保存DayVideoAccessStat到数据库
   *
   * @param list
   */
  def insertNetTypeAccessTopN(list: ListBuffer[DayNetTypeAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()

      // 设置手动提交
      connection.setAutoCommit(false)
      val sql = "insert into day_netType_access_topn_stat (day, uid, times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.uid)
        pstmt.setLong(3, ele.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      // 手动提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }

  /**
   * 批量保存DayCityNetTypeAccessStat到数据库
   *
   * @param list
   */
  def insertDayNetTypeCityAccessTopN(list: ListBuffer[DayCityNetTypeAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()

      // 设置手动提交
      connection.setAutoCommit(false)
      val sql = "insert into day_netType_city_access_topn_stat (day, uid, city, times, times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.uid)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setLong(5, ele.times_rank)
        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      // 手动提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }

  /**
   * 批量保存DayNetTypeTrafficsStat到数据库
   *
   * @param list
   */
  def insertDayNetTypeTrafficsAccessTopN(list: ListBuffer[DayNetTypeTrafficsStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()

      // 设置手动提交
      connection.setAutoCommit(false)
      val sql = "insert into day_netType_city_traffics_topn_stat (day, uid, traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.uid)
        pstmt.setLong(3, ele.traffics)
        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      // 手动提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }

  /**
   * 删除指定日期的数据
   * @param day
   */
  def deleteDay(day:String): Unit ={
    val tables = Array("day_netType_access_topn_stat", "day_netType_city_access_topn_stat", "day_netType_city_traffics_topn_stat")
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()

      for (table <-tables){
        // 相当于 "delete from table where day = ?"
        val deleteSQL = s"delete from $table where day = ?"
        val pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }

}
