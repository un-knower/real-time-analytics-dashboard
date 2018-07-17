name := "RealTime_Load"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

publishMavenStyle := true

organization := "com.indeed.dataengineering"

crossPaths := false

publishArtifact in(Compile, packageSrc) := true

val sparkVersion = "2.3.0"

val sparkDependencyScope = "provided"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-yarn" % sparkVersion % sparkDependencyScope,
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe" % "config" % "1.3.1",
  //	"com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "org.apache.hadoop" % "hadoop-distcp" % "2.8.2" % sparkDependencyScope,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "0.10.0.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  //	"org.apache.hadoop" % "hadoop-tools" % "1.2.1" % sparkDependencyScope
  //	"org.apache.hadoop" % "hadoop-core" % "1.2.1",
  //	"org.apache.hadoop" % "hadoop-common" % "2.8.2"
)


// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.224"


updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", _@_ *) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
