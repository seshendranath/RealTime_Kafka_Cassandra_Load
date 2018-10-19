package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 10/19/17.
  */


import java.sql.ResultSet
import javax.sql.rowset.CachedRowSet
import com.sun.rowset.CachedRowSetImpl
import java.sql.DriverManager


class PostgreSql(url: String, user: String, password: String) {

  val driver = "org.postgresql.Driver"

  Class.forName(driver)

  val rdb = "postgresql"

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
      val rs = statement.executeQuery(query)
      rowset.populate(rs)

    } catch {

      case e: Exception => e.printStackTrace()

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
      rs = statement.executeUpdate(query)

    } catch {
      case e: Exception => e.printStackTrace()

    } finally {
      conn.close()
    }

    rs
  }

}