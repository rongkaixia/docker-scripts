name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.3"

// additional libraries
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1" withSources() withJavadoc()
)
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
/*
  "org.apache.spark" %% "spark-hive" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-flume" % "1.3.1",
  "org.apache.spark" %% "spark-mllib" % "1.3.1",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  "com.typesafe.play" % "play-json_2.10" % "2.2.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.3",
  "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.0.0.RC1",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.twitter.elephantbird" % "elephant-bird" % "4.5",
  "com.twitter.elephantbird" % "elephant-bird-core" % "4.5",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17",
  "mysql" % "mysql-connector-java" % "5.1.31",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-rc5",
  "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.0.0-rc5",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "0.0.1" % "test"
 */