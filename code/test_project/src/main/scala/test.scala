// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._

// sqlContext.sql("""CREATE KEYSPACE test USING org.apache.spark.sql.cassandra""".stripMargin)
// sqlContext.sql("""CREATE TEMPORARY TABLE words USING org.apache.spark.sql.cassandra OPTIONS (table "words", keyspace "mykeyspace", pushdown "true")""".stripMargin)


class SearchEngine {


}
/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    // val conf = new SparkConf(true).setAppName("test cassandra").set("spark.cassandra.connection.host","cassandra")
    val conf = new SparkConf(true).setAppName("test cassandra")
    .set("spark.cassandra.connection.timeout_ms", "600000")
    .set("spark.cassandra.read.timeout_ms", "600000")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // val df = sc.cassandraTable("chinamarket", "daymarketdata").toArray()
    // println(df)
    // val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("cars.csv")
    val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "daymarketdata", "keyspace" -> "chinamarket" )).load()
    // df.write.cassandraFormat("words_copy", "test", "cluster_B").save()
    // df.show()
    println("Done!")
    sc.stop()
  }
}
// scalastyle:on println