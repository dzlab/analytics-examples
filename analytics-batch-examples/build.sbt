name := "storage-layer"

version := "0.1"

scalaVersion := "2.10.5" //scalaVersion := "2.11.4"

javaOptions += "-Xmx1G"

libraryDependencies += "org.mongodb" %% "casbah" % "2.8.2"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
libraryDependencies += "com.twitter" %% "finagle-httpx" % "6.27.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.2"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
//libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"
libraryDependencies += "nl.grons" %% "metrics-scala" % "3.5.2_a2.3"
