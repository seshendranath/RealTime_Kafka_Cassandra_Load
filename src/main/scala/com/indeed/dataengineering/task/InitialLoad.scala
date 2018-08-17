package com.indeed.dataengineering.task


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.datastax.driver.core._
import com.indeed.dataengineering.utilities.Utils._

class InitialLoad {

  def run(): Unit = {

    val cassandraHosts = conf("cassandra.host").split(",").toSeq
    val cluster = Cluster.builder().addContactPoints(cassandraHosts: _*).build()
    val session = cluster.connect(conf("cassandra.keyspace"))

    spark.conf.set("spark.cassandra.connection.host", conf("cassandra.host"))
    spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

    val db = conf("db")
    val table = conf("table")
    val pk = conf("pk")

    val df = spark.read.parquet(conf("s3aUri") + conf("s3Bucket") + "/" + conf("basePath") + "/" + s"$db/$table")
    val df1 = df.repartition(160)
    df1.persist

    val cols = getColsFromDF(df)

    if (conf.getOrElse("drop", "false") == "true") {
      val query = s"DROP TABLE IF EXISTS $db.$table;"
      session.execute(query)
    }

    val query = s"CREATE TABLE IF NOT EXISTS $db.$table (" + cols.map(x => x._1 + " " + x._2).mkString(",") + s", PRIMARY KEY ($pk));"
    session.execute(query)

    if (conf.getOrElse("load", "false") == "true") {
      df1.count
      var df2 = df1
      for (c <- df1.columns) df2 = df2.withColumnRenamed(c, c.toLowerCase)
      df2.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> table.toLowerCase, "keyspace" -> db)).save

    }

  }

}
