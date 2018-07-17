package com.indeed.dataengineering.config

/**
  * Created by aguyyala on 2/16/17.
  */


import scopt.OptionParser
import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Extraction}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source


object AnalyticsTaskConfig {

	def parseCmdLineArguments(args: Array[String]): Map[String, String] = {

		val parser = new OptionParser[Config]("Job") {
			head("Job")

			opt[String]('e', "env").required().action((x, c) => c.copy(env = x))
				.validate(x =>
					if (Seq("local", "dev", "stage", "prod") contains x) success
					else failure("Invalid env: only takes dev, stage, or prod"))
				.text("Environment in which you want to run dev|stage|prod")

			opt[Unit]("debug").action { (_, c) =>
				c.copy(debug = "true")
			}.text("Debug Flag")

			help("help").text("Prints Usage Text")

			override def showUsageOnError = true

			override def errorOnUnknownArgument = false

			override def reportError(msg: String): Unit = {
				showUsageAsError
				Console.err.println("Error: " + msg)
				terminate(Left(msg))
			}

			override def reportWarning(msg: String): Unit = {}
		}

		val pConf = parser.parse(args, Config()).getOrElse(Config())
		if (pConf.debug == "true") pConf.logLevel = "Debug"

		var conf = mutable.Map(Extraction.decompose(pConf)(DefaultFormats).values.asInstanceOf[Map[String, String]].toSeq: _*)

		def getConf(confFile: String): Map[String, String] = ConfigFactory.parseResourcesAnySyntax(confFile).entrySet.asScala.map(e => (e.getKey, e.getValue.unwrapped.toString)).toMap

		Source.fromInputStream(getClass.getResourceAsStream("/loadConf.txt")).getLines.foreach{confFile =>
			conf ++= getConf(confFile)
		}

		val otherProperties = args.map(x => x.split("=", 2)).map {
			case Array(a, b) => (a.toString.stripPrefix("--"), b)
			case Array(a) => (a.toString.stripPrefix("--"), "true")
		}.toMap

		if (pConf.env == "local") {
			val sparkLocalProperties = getConf("spark-local.properties")
			conf ++= sparkLocalProperties
		}

		conf ++= otherProperties

		val convert = Map("env" -> pConf.env)

		implicit class StringImprovements(val s: String) {
			def richFormat(replacement: Map[String, Any]): String =
				(s /: replacement) { (res, entry) => res.replaceAll("#\\{%s\\}".format(entry._1), entry._2.toString) }
		}

		conf.map { case (k, v) => k -> (v richFormat convert) }.toMap

	}


	case class Config(env: String = "stage", debug: String = "false", var logLevel: String = "Info")

}
