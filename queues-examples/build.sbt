name := "endpoint"

version := "0.1"

scalaVersion := "2.10.5"
//scalaVersion := "2.11.7"

resolvers ++= Seq(
   "Akka Snapshot Repository"         at "http://repo.akka.io/snapshots/",
   "Typesafe Repository"              at "http://repo.typesafe.com/typesafe/releases/",
   Resolver.bintrayRepo("scalaz", "releases")
 )

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.10.27"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-kinesis" % "1.10.27"

libraryDependencies += "io.github.cloudify" %% "scalazon" % "0.11"
libraryDependencies += "com.twitter" %% "util-collection" % "latest.release"
libraryDependencies += "com.twitter" %% "finagle-http" % "latest.release"
libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "latest.release"
libraryDependencies += "com.github.tyagihas" %% "scala_nats" % "0.1"
libraryDependencies += "com.github.cloudfoundry-community" % "nats-client" % "0.6.3"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT"

libraryDependencies += "com.github.seratch" %% "awscala" % "0.5.+"

libraryDependencies += "org.json4s" %% "json4s-native" % "latest.version"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "latest.version"

libraryDependencies += "joda-time" % "joda-time" % "2.9"
libraryDependencies += "nl.grons" %% "metrics-scala" % "3.5.2_a2.3"
