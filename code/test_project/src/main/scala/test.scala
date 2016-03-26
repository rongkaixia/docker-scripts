// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}
// sqlContext.sql("""CREATE KEYSPACE test USING org.apache.spark.sql.cassandra""".stripMargin)
// sqlContext.sql("""CREATE TEMPORARY TABLE words USING org.apache.spark.sql.cassandra OPTIONS (table "words", keyspace "mykeyspace", pushdown "true")""".stripMargin)

// each row of A is an observation
def normalization(A: DenseMatrix[Double]): DenseMatrix[Double] = {
    val _mean = mean(A(::,*))
    val _std = stddev(A(::,*))
    var i = 0
    var ret = new DenseMatrix[Double](A.rows, A.cols, A.toArray)
    for(i <- 0 to A.rows - 1){
        ret(i,::) -= _mean
    }
    return ret
}

def euclideanDistanceKernel(A: DenseMatrix[Double]): DenseMatrix[Double] = {
    // sum(A, 1) - 2AA^T + sum(A, 1)^T
    var ret = -2.0 * a * a.t
    val _sum2 = sum(pow(A,2),Axis._1)
    var i = 0
    for(i <- 0 to A.rows - 1){
        ret(i,::) += _sum2.t + _sum2(i)
    }
    return sqrt(ret)
}

def centerDistance(A: DenseMatrix[Double]): DenseMatrix[Double] = {
    var dist = euclideanDistanceKernel(A)
    val allMean = mean(dist)
    val rowMean = mean(dist(*,::))
    val colMean = mean(dist(::,*))
    var i = 0
    for(i <- 0 to dist.rows - 1){
        dist(i,::) += (-colMean - rowMean(i) + allMean)
    }
    return dist
}

def brownianCov(A: DenseMatrix[Double], B: DenseMatrix[Double]): Double = {
    val Adist = centerDistance(euclideanDistanceKernel(A))
    val Bdist = centerDistance(euclideanDistanceKernel(B))
    return sqrt(sum(Adist :* Bdist)) / Adist.rows
}

def brownianCorrelation(A: DenseMatrix[Double], B: DenseMatrix[Double]): Double = {
    return brownianCov(A,B)/sqrt(brownianCov(A,A)*brownianCov(B,B))
}

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
  sc.pali
}
// scalastyle:on println