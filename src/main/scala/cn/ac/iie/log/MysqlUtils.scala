package cn.ac.iie.log

import java.sql.{Connection, DriverManager, PreparedStatement}


object MysqlUtils {
  def getConnection() ={
    DriverManager.getConnection("jdbc:mysql://swarm-manager:3306/imooc_project?user=root&password=123456")

  }

  /**
   * 释放数据库连接资源
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit={
    try{
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
