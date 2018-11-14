name := "roowhoo-dashboard"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.0"
libraryDependencies += "org.rogach" %% "scallop" % "3.0.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.7.1"
libraryDependencies += "com.typesafe.akka" % "akka-http_2.12" % "10.0.10"
libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.2.1"
enablePlugins(JavaAppPackaging)
