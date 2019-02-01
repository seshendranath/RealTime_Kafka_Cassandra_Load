package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.LoadConfig.conf
import com.indeed.dataengineering.utilities.{Logging, SqlJDBC}


class Dummy extends Logging {

  def run(): Unit = {

    log.info("Running Dummy Class...")
    log.info("Creating Redshift connection")
    val redshift = new SqlJDBC("redshift", conf("redshift.url"), conf("redshift.user"), conf("redshift.password"))

    val res = redshift.executeQuery("select count(*) from source_s3.tblADCquota")
    println(s"TEST: $res")

  }

}


