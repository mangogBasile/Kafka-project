name := "kafkatest"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies +=  "org.apache.kafka"  % "kafka_2.12"  % "1.0.0"
libraryDependencies +=   "org.apache.kafka" % "kafka-streams" % "1.0.0"

libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.10"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.4"
libraryDependencies += "com.typesafe.akka" %% "akka-actor"  % "2.5.4"