package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 10/19/17.
  */


import java.sql.ResultSet
import javax.sql.rowset.CachedRowSet
import com.sun.rowset.CachedRowSetImpl
import java.sql.DriverManager


class SqlJDBC(rdb: String, url: String, user: String, password: String) extends Logging {

  val driverMap = Map("postgresql" -> "org.postgresql.Driver", "redshift" -> "com.amazon.redshift.jdbc.Driver")
  val driver = driverMap(rdb)

  Class.forName(driver)

  /**
    * Executes MySql Server Query
    *
    * @param query : Query to be executed
    * @return Result Set of the query
    */
  def executeQuery(query: String): CachedRowSet = {

    val conn = DriverManager.getConnection(url, user, password)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val rowset: CachedRowSet = new CachedRowSetImpl

    try {
      log.info(s"$rdb - Running Query: $query")
      val rs = statement.executeQuery(query)
      rowset.populate(rs)

    } catch {

      case e: Exception => e.printStackTrace(); throw e

    } finally {
      conn.close()
    }

    rowset
  }


  /**
    * Executes Update, Delete, Insert Queries of MySQL Server
    *
    * @param query : Query to be executed
    * @return No. of rows updated by that query
    */
  def executeUpdate(query: String): Int = {

    val conn = DriverManager.getConnection(url, user, password)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    var rs: Int = -1

    try {
      log.info(s"$rdb - Running Query: $query")
      rs = statement.executeUpdate(query)

    } catch {
      case e: Exception => e.printStackTrace(); throw e

    } finally {
      conn.close()
    }

    rs
  }

}