package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.datastax.driver.core.Cluster
import com.indeed.dataengineering.AnalyticsTaskApp._
import com.datastax.spark.connector.cql.CassandraConnector
import com.indeed.dataengineering.utilities.Logging

import collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql.DataFrame

import scala.language.reflectiveCalls


class Generic extends Logging {

  def run(): Unit = {

    val runClass = conf("runClass")

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))

    val skipMetadata = conf.getOrElse("skipMetadata", "false").toBoolean

    val (assignoption, assignString, offsetString, partitions) = if (skipMetadata) {
      (conf.getOrElse("assignString", "subscribe"), topics, conf.getOrElse("offsetString", "latest"), conf.get("partitions").filter(_ != "").map(_.split(",")).getOrElse(Array()).map(_.toInt).toSet)
    } else {
      log.info("Connect to cassandra cluster")
      val cluster = Cluster.builder().addContactPoints(conf("cassandra.host").split(","): _*).build()
      val session = cluster.connect("metadata")

      val query = s"select topic, partition, offset from streaming_metadata where job = '${runClass.split("\\.").last}';"
      log.info(s"Running Query in Cassandra to fetch partitions and offsets: $query")
      val res = session.execute(query).all.asScala.toArray

      val resMap = mutable.Map[String, mutable.Map[Int, Long]]()

      val partitions = res.map(_.getInt("partition")).toSet

      res.foreach { rec =>
        val topic = rec.getString("topic")
        val partition = rec.getInt("partition")
        val offset = if (rec.getLong("offset") == -1) rec.getLong("offset") else rec.getLong("offset") + 1
        val value = resMap.getOrElse(topic, mutable.Map[Int, Long]())

        val existingOffset = value.getOrElse(partition, offset)
        val finalOffset = if (offset != -1 && existingOffset != -1) Math.min(existingOffset, offset)
        else if (offset != -1) offset
        else existingOffset

        resMap += topic -> (value + (partition -> finalOffset))
      }

      val aString = "{" + resMap.map { case (k, v) => s""""$k":[${v.keys.mkString(",")}]""" }.mkString(",") + "}"
      val oString = "{" + resMap.map { case (k, v) => s""""$k":{${v.map { case (p, o) => '"' + s"$p" + '"' + s":$o" }.mkString(",")}}""" }.mkString(",") + "}"

      ("assign", aString, oString, partitions)
    }

    log.info("Read Kafka streams")
    log.info(s"Assign following topics and partitions: $assignString")
    log.info(s"Starting from the following offsets: $offsetString")

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option(assignoption, assignString)
      .option("failOnDataLoss", conf.getOrElse("failOnDataLoss", "false"))
      .option("kafka.max.partition.fetch.bytes", conf.getOrElse("max.partition.fetch.bytes", "15728640").toInt)

    val kafkaStream = if (!conf.getOrElse("checkpoint", "false").toBoolean || conf.getOrElse("offsetString", "").nonEmpty) stream.option("startingOffsets", offsetString).load else stream.load

    //.option("subscribe", topics)
    //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)
    //.option("assign", """{"maxwell":[4,7,1,9,3]}""")
    //.option("kafka.max.partition.fetch.bytes", conf.getOrElse("max.partition.fetch.bytes", "2147483647").toInt)
    //.option("kafka.max.poll.records", conf.getOrElse("max.poll.records", "2147483647").toInt)
    //.option("kafka.request.timeout.ms", conf.getOrElse("request.timeout.ms", "40000").toInt)
    //.option("kafka.session.timeout.ms", conf.getOrElse("session.timeout.ms", "30000").toInt)
    //.option("kafka.fetch.max.bytes", conf.getOrElse("fetch.max.bytes", "2147483647").toInt)

    log.info("Extract value and map from Kafka consumer records")
    val rawData = kafkaStream.selectExpr("topic", "partition", "offset", "timestamp AS kafka_timestamp", "CAST(value AS STRING)")

    log.info(s"Running $runClass...")
    val clazz = Class.forName(runClass).newInstance.asInstanceOf[ {def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit}]
    clazz.run(rawData, connector, partitions)
  }

}


