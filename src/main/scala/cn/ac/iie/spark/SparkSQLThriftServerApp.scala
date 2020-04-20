package cn.ac.iie.spark

import java.sql.DriverManager

/**
 * 通过JDBC 方式
 */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://manager:10000", "iie4bu", "")
    val pstmt = conn.prepareStatement("select transactionid, customerid from sal")
    val rs = pstmt.executeQuery()
    while(rs.next()) {
      println("transactionid:" + rs.getInt("transactionid") + ", customerid:" + rs.getString("customerid"))
    }
    rs.close()
    pstmt.close()
  }

}
