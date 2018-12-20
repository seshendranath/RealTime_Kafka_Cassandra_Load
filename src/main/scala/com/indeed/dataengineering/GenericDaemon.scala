package com.indeed.dataengineering


/**
  * Created by aguyyala on 10/19/17.
  */


//import java.net.URI
//import com.amazonaws.services.s3.AmazonS3ClientBuilder
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.BasicAWSCredentials
import com.indeed.dataengineering.utilities.Logging
//import config.AnalyticsTaskConfig

import scala.io.Source


/*
 * Main class and entry point to the application
 */
object GenericDaemon extends App with Logging {

  val conf: Map[String, String] = LoadConfig(args)

  lazy val s3 = if (conf("env") == "dev") {
    val awsCreds = new BasicAWSCredentials(conf("accessKey"), conf("secretKey"))
    AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build
  } else AmazonS3ClientBuilder.defaultClient

  //  lazy val tx = TransferManagerBuilder.defaultTransferManager

  Source.fromInputStream(getClass.getResourceAsStream("/banner.txt")).getLines.foreach(println)

  val s = System.nanoTime()

  try {

    val classes = conf("class")
    classes.split(",").map(_.trim).foreach { cName =>
      val clazz = getClass.getClassLoader.loadClass(cName)
      clazz.getMethod("run").invoke(clazz.newInstance)
    }
  }
  catch {
    case e: Throwable => errorHandler(e)
  }

  val e = System.nanoTime()
  val totalTime = (e - s) / (1e9 * 60)
  log.info("Total Elapsed time: " + f"$totalTime%2.2f" + " mins")


  def errorHandler(e: Throwable): Unit = {
    log.error(s"Something went WRONG during the run for Instance")
    log.error(e.printStackTrace())
    System.exit(1)
  }

  //    log.info("Cleaning up CheckPoint Directory")

  //    log.info("Shutting Down AWS S3 Transfer Manager")
  //		tx.shutdownNow()

}
