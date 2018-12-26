name := "KafkaOffsetMonitor"
version := "0.4.7-dmilan-SNAPSHOT"
scalaVersion := "2.11.12"
organization := "com.quantifind"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-optimize", "-feature")

mainClass in Compile := Some("com.quantifind.kafka.offsetapp.OffsetGetterWeb")

libraryDependencies ++= Seq(
	"log4j" % "log4j" % "1.2.17",
	"net.databinder" %% "unfiltered-filter" % "0.8.4",
	"net.databinder" %% "unfiltered-jetty" % "0.8.4",
	"net.databinder" %% "unfiltered-json4s" % "0.8.4",
	"com.quantifind" %% "sumac" % "0.3.0",
	"org.apache.kafka" %% "kafka" % "1.1.0",
	"org.reflections" % "reflections" % "0.9.11",
	"com.twitter" %% "util-core" % "7.1.0",
	"com.typesafe.slick" %% "slick" % "2.1.0",
	"org.xerial" % "sqlite-jdbc" % "3.25.2",
	"com.google.code.gson" % "gson" % "2.8.2",
	"com.google.guava" % "guava" % "20.0",
	"javax.ws.rs" % "javax.ws.rs-api" % "2.0-m16",
	"org.glassfish.jersey.core" % "jersey-client" % "2.25.1",
	"org.mockito" % "mockito-all" % "1.10.19" % "test",
	"org.scalatest" %% "scalatest" % "2.2.6" % "test")

assemblyMergeStrategy in assembly := {
  case "about.html" => MergeStrategy.discard
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}