name := "roowhoo-dashboard"

version := "1.0"

scalaVersion := "2.12.3"
lazy val root = (project in file(".")).enablePlugins(PlayScala)


libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.0"
libraryDependencies += "org.rogach" %% "scallop" % "3.0.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.7.1"
libraryDependencies += "com.typesafe.akka" % "akka-http_2.12" % "10.0.10"
libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.2.1"
libraryDependencies += guice
libraryDependencies +=  "org.webjars" % "bootstrap" % "3.0.3" exclude("org.webjars", "jquery")
libraryDependencies += "org.webjars" % "bootstrap-datetimepicker" % "2.4.2" exclude("org.webjars", "bootstrap")
libraryDependencies += "org.webjars" % "jquery" % "2.2.4"
enablePlugins(JavaAppPackaging)
enablePlugins(PlayScala)