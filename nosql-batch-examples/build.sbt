name := "nosql-batch-examples"

version := "0.1"

scalaVersion := "2.10.5"

resolvers ++= Seq(
   "Typesafe repository snapshots"    at "http://repo.typesafe.com/typesafe/snapshots/",
   "Typesafe repository releases"     at "http://repo.typesafe.com/typesafe/releases/",
   "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
   "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
   "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
   "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
   "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
   "Twitter Repository"               at "http://maven.twttr.com",
   "Sonatype OSS Snapshots"           at "https://oss.sonatype.org/content/repositories/snapshots",
   "Couchbase Repository"             at "http://files.couchbase.com/maven2/",
   "clojars"                         at "http://clojars.org/repo/",
    Resolver.bintrayRepo("websudos", "oss-releases")
 )

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0" % "provided"
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "1.0.0-beta" % "provided"
libraryDependencies += "com.couchbase.client" % "java-client" % "2.2.0" % "provided"
libraryDependencies += "com.aerospike" % "aerospike-client" % "latest.integration" % "provided"
libraryDependencies += "com.basho.riak" % "riak-client" % "2.0.0" % "provided"
libraryDependencies += "monetdb" % "monetdb-jdbc" % "2.8" % "provided"
//libraryDependencies += "com.basho" % "spark-riak-connector" % "1.0.0"
//libraryDependencies += "org.influxdb" % "influxdb-java" % "2.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11" % "provided"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
//libraryDependencies += "com.websudos" %% "phantom-dsl" % "1.12.2"
libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"
libraryDependencies ++= Seq(
  ("nl.grons" %% "metrics-scala" % "3.5.2_a2.3").
  exclude("com.codahale.metrics", "metrics-core").
  exclude("io.dropwizard.metrics", "metrics-core")
)
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7"
libraryDependencies += "com.google.guava" % "guava" % "16.0.1" 
libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.2.1"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.2.0" % "provided"
// resolve conflict from netty jars (handler, buffer, common, transport, codec)
assemblyMergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

