name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.3"

// additional libraries
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1",

  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalanlp" %% "breeze-viz" % "0.12"
)

resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

/*
assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
*/

assemblyMergeStrategy in assembly := {
  case x if x.endsWith(".class") => MergeStrategy.last
  case x if x.endsWith(".properties") => MergeStrategy.last
  case x if x.contains("/resources/") => MergeStrategy.last
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
  case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
  case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy == MergeStrategy.deduplicate)
      MergeStrategy.first
    else
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