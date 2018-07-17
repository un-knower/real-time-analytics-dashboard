package com.indeed.dataengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
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

    import spark.implicits._

    val Array(brokers, topics) = Array("ec2-54-85-62-208.compute-1.amazonaws.com:9092", "test")
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

    log.info("Read Kafka streams")
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", """ {"test":{"0":-1}} """)
      .load()

    log.info("Extract value and map from Kafka consumer records")
    val ds1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2).map(_.split(","))

    log.info("Map extracted kafka consumer records to Click Analytics Case Class")
    val ds2 = ds1.map(x => ClickAnalytics(new Timestamp(format.parse(x(0)).getTime), x(1), x(2)))

    log.info("Create Cassandra connector by passing host as ec2-54-85-62-208.compute-1.amazonaws.com")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", "ec2-54-85-62-208.compute-1.amazonaws.com"))

    log.info("Create ForeachWriter for Cassandra")
    val writer = new ForeachWriter[ClickAnalytics] {

      def open(partitionId: Long, version: Long): Boolean = {
        true
      }

      def process(value: ClickAnalytics): Unit = {
        val q = s"INSERT INTO test.click_analytics (click_time, country, city) VALUES ('${value.timestamp}', '${value.country}', '${value.city}')"
        connector.withSessionDo(session => session.execute(q))
      }

      def close(errorOrNull: Throwable): Unit = {}
    }

    log.info("Write Streams to Cassandra Table")
    val query = ds2.writeStream.foreach(writer).outputMode("append").start

    log.info("Await Query Termination")
    query.awaitTermination()

  }

}


