import sbt.Keys._


name := "RISK_GRAPHX_MX"

version := "1.0"

scalaVersion in ThisBuild := "2.10.4"

//jarName in assembly := "graphx.jar"

val meta = """META.INF(.)*""".r


startYear := Some(2017)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.2" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "0.1" % "provided"


libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2"
)

