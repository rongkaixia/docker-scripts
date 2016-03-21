// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

import com.datastax.spark.connector._

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    // val conf = new SparkConf(true).setAppName("test cassandra").set("spark.cassandra.connection.host","cassandra")
    val conf = new SparkConf(true).setAppName("test cassandra")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "user", "keyspace" -> "tutorial" )).load()
    df.show()
    println("Done!")
    sc.stop()
  }
}
// scalastyle:on println