package com.indeed.dataengineering

/**
  * Created by aguyyala on 10/19/17.
  */


import java.net.URI

//import com.amazonaws.services.s3.AmazonS3ClientBuilder
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.indeed.dataengineering.utilities.Logging
import org.apache.hadoop.fs.{FileContext, FileSystem}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.sql.SparkSession
import config.AnalyticsTaskConfig

import scala.io.Source


/*
 * Main class and entry point to the application
 */
object AnalyticsTaskApp extends App with Logging {

  lazy val spark: SparkSession = getSparkSession
  lazy val conf: Map[String, String] = AnalyticsTaskConfig.parseCmdLineArguments(args)
  conf.foreach { case (k, v) => spark.conf.set(k, v) }
  //	setPropertiesFromGit(spark)

  lazy val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  lazy val dfc = FileContext.getFileContext(spark.sparkContext.hadoopConfiguration)
  lazy val s3fs = FileSystem.get(new URI(conf("s3aUri") + conf("s3Bucket")), spark.sparkContext.hadoopConfiguration)
  lazy val s3Stagefs = FileSystem.get(new URI(conf("s3aUri") + conf("s3StageBucket")), spark.sparkContext.hadoopConfiguration)
  lazy val hiveMetaStore = new HiveMetaStoreClient(new HiveConf())
//  lazy val s3 = AmazonS3ClientBuilder.defaultClient()
//  lazy val tx = TransferManagerBuilder.defaultTransferManager

  spark.sparkContext.setLogLevel("Warn")

  val checkPointDir = dfs.getHomeDirectory.toString + "/" + spark.sparkContext.applicationId
  spark.sparkContext.setCheckpointDir(checkPointDir)

  Source.fromInputStream(getClass.getResourceAsStream("/banner.txt")).getLines.foreach(println)

  log.info("=" * 100 + " " + spark.sparkContext.applicationId + " " + "=" * 100)

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
  finally {
    cleanup()
  }

  val e = System.nanoTime()
  val totalTime = (e - s) / (1e9 * 60)
  log.info("Total Elapsed time: " + f"$totalTime%2.2f" + " mins")

  def getSparkSession: SparkSession = {
    SparkSession
      .builder
      .enableHiveSupport()
      .getOrCreate()
  }


  def errorHandler(e: Throwable): Unit = {
    log.error(s"Something went WRONG during the run for Instance")
    log.error(e.printStackTrace())
    System.exit(1)
  }


  def cleanup(): Unit = {
    log.info("Stopping Spark Session")
    try {
      spark.stop()
    } catch {
      case _: Throwable =>
    }

    log.info("Cleaning up CheckPoint Directory")

    log.info("Shutting Down AWS S3 Transfer Manager")
    //		tx.shutdownNow()
  }

}
