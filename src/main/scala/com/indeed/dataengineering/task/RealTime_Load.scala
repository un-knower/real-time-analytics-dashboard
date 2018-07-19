package com.indeed.dataengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.window
import java.sql.Timestamp
import java.text.SimpleDateFormat
import com.datastax.spark.connector.cql.CassandraConnector
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
//import org.apache.spark.sql.SparkSession
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import java.util.Date
//import org.apache.spark.sql.Row
//import com.datastax.driver.core._

class RealTime_Load {

  def run(): Unit = {

    val sql = spark.sql _

    import spark.implicits._

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))
    val keyspace = conf("cassandra.keyspace")

    log.info("Read Kafka streams")
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()
      //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)

    log.info("Extract value and map from Kafka consumer records")
    val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2).map(_.split(","))

    log.info("Map extracted kafka consumer records to Click Case Class")
    val clickRawDataDS = rawData.map(x => ClickRawData(new Timestamp(timestampFormat.parse(x(0)).getTime), x(1), x(2)))

    val clickRawData = clickRawDataDS.toDF
    clickRawData.createOrReplaceTempView("clickRawData")

    val query =
      """
        |SELECT
        |      timestamp
        |     ,country
        |     ,city
        |     ,date_format(timestamp, 'MMMMM') AS month
        |     ,date_format(timestamp, 'EEEE') AS day
        |     ,year(timestamp) AS year
        |     ,hour(timestamp) AS hour
        |     ,weekofyear(timestamp) AS week
        |     ,dayofyear(timestamp) AS day_of_year
        |     ,dayofmonth(timestamp) AS day_of_month
        |FROM clickRawData
      """.stripMargin
    val massagedClickData = sql(query)


    log.info("Create ForeachWriter for Cassandra")
    val clickWriter = new ForeachWriter[Click] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: Click): Unit = {
        val cQuery1 = s"INSERT INTO $keyspace.click_raw_data (click_time, country, city) VALUES ('${value.timestamp}', '${value.country}', '${value.city}')"
        val cQuery2 = s"UPDATE $keyspace.click_count_by_day SET click_count = click_count + 1 WHERE day = '${value.day}'"
        val cQuery3 = s"UPDATE $keyspace.click_count_by_hour SET click_count = click_count + 1 WHERE hour = ${value.hour}"
        val cQuery4 = s"UPDATE $keyspace.click_count_by_month SET click_count = click_count + 1 WHERE month = '${value.month}'"
        val cQuery5 = s"UPDATE $keyspace.click_count_by_country_city SET click_count = click_count + 1 WHERE country = '${value.country}' AND city = '${value.city}'"
        val cQuery6 = s"UPDATE $keyspace.click_count_by_week SET click_count = click_count + 1 WHERE week = ${value.week}"
        val cQuery7 = s"UPDATE $keyspace.click_count_by_day_of_year SET click_count = click_count + 1 WHERE day_of_year = ${value.day_of_year}"
        val cQuery8 = s"UPDATE $keyspace.click_count_by_day_of_month SET click_count = click_count + 1 WHERE day_of_month = ${value.day_of_month}"
        val cQuery9 = s"UPDATE $keyspace.click_count_by_year SET click_count = click_count + 1 WHERE year = ${value.year}"

        connector.withSessionDo{session =>
          session.execute(cQuery1)
          session.execute(cQuery2)
          session.execute(cQuery3)
          session.execute(cQuery4)
          session.execute(cQuery5)
          session.execute(cQuery6)
          session.execute(cQuery7)
          session.execute(cQuery8)
          session.execute(cQuery9)
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }

    log.info("Write Streams to Cassandra Table")
    val streamQuery = massagedClickData.as[Click].writeStream.foreach(clickWriter).outputMode("append").start


    log.info("Create LiveCount ForeachWriter for Cassandra")
    val liveCountWriter = new ForeachWriter[LiveClickCount] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: LiveClickCount): Unit = {
        val cQuery1 = s"INSERT INTO $keyspace.click_count_by_interval(click_time, click_count) VALUES ('${value.timestamp}', ${value.click_count})"
        connector.withSessionDo(session => session.execute(cQuery1))
      }

      def close(errorOrNull: Throwable): Unit = {}
    }

    log.info("Calculate click count by 1 sec interval")
    val windowedCounts = clickRawDataDS.toDF.withWatermark("timestamp", "10 minutes").groupBy(window($"timestamp", "1 second", "1 second")).count()

    log.info("Write Live Count to Cassandra Table")
    val liveCountQuery = windowedCounts.selectExpr("window.end AS timestamp", "CAST(count AS Int) AS click_count").as[LiveClickCount].writeStream.outputMode("update").foreach(liveCountWriter).start

    if (conf.getOrElse("debug", "false") == "true") {
      val debugQuery = massagedClickData.writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()
      log.info("Await debugQuery Termination")
      debugQuery.awaitTermination()
    }

    log.info("Await liveCountQuery Termination")
    liveCountQuery.awaitTermination()

    log.info("Await streamQuery Termination")
    streamQuery.awaitTermination()



  }

}


