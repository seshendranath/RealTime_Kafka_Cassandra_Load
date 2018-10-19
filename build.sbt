name := "RealTime_Load"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

publishMavenStyle := true

organization := "com.indeed.dataengineering"

crossPaths := false

publishArtifact in(Compile, packageSrc) := true

val sparkVersion = "2.3.1"
val kafkaVersion = "0.10.0.1"
val awsVersion = "1.11.383"
val jsonVersion = "3.6.0"

val sparkDependencyScope = "provided"


libraryDependencies ++= Seq(
  // Spark Libraries
  "org.apache.hadoop" % "hadoop-distcp" % "2.8.2" % sparkDependencyScope,
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-yarn" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkDependencyScope,

  // Config Libraries
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",

  // Kafka Libraries
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // Json Libraries
  "org.json4s" %% "json4s-native" % jsonVersion,

  // Cassandra Libraries
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,

  // AWS Libraries
  "com.amazonaws" % "aws-java-sdk" % awsVersion % sparkDependencyScope,
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-core" % awsVersion,
  "org.apache.hadoop" % "hadoop-aws" % "2.8.1"
    exclude("javax.servlet",     "servlet-api")
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("org.mortbay.jetty", "servlet-api"),

  // Twitter Libraries
  "com.twitter" % "jsr166e" % "1.1.0",

  // Testing only
  "org.scala-lang" % "scala-xml" % "2.11.0-M4",

  // Postgresql Driver
  "org.postgresql" % "postgresql" % "42.2.2"

)


// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.224"


updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", _@_ *) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
