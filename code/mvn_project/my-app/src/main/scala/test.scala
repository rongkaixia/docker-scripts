// scalastyle:off println
package com.mycompany.app

import scala.math.random

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}

object Stat {
    // each row of A is an observation
    def normalization(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        val _mean = mean(A(::,*)).toDenseVector
        // val _mean = mean(A(::,*)).t
        val _std = stddev(A(::,*))
        var i = 0
        var ret = new DenseMatrix[Double](A.rows, A.cols, A.toArray)
        ret(*,::) -= _mean
        return ret
    }

    def euclideanDistanceKernel(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        // sum(A, 1) - 2AA^T + sum(A, 1)^T
        var ret = -2.0 * A * A.t
        val _sum2 = sum(pow(A,2),Axis._1)
        ret(*,::) += _sum2
        ret(::,*) += _sum2
        return sqrt(ret)
    }

    def centerDistance(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        var dist = euclideanDistanceKernel(A)
        val allMean = mean(dist)
        val rowMean = mean(dist(*,::))
        val colMean = mean(dist(::,*)).toDenseVector
        // val colMean = mean(dist(::,*)).t
        dist(*,::) -= rowMean
        dist(::,*) -= colMean
        dist += allMean
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

}

object Context{
    var sparkContext: SparkContext = _

    var sqlContext: SQLContext = _

    var cassandraContext: CassandraSQLContext = _
}

object Utils{
    def CassandraTableScanRDD2DoubleArray(table: com.datastax.spark.connector.rdd.CassandraTableScanRDD[CassandraRow]): Array[Array[Double]] = {
        return table.map(row => {
                var ret = new Array[Double](row.size)
                var i = 0
                for(i <- 0 to row.size - 1){
                    ret(i) = row.getDouble(i)
                }
                ret
            }).collect
    }
}

object SearchEngine{
    // def _row2DoubleArray(row: org.apache.spark.sql.Row){
    //     var ret = new Array[Double](row.size)
    // }

    def doMatch(cass:com.datastax.spark.connector.rdd.CassandraTableScanRDD[CassandraRow], row: org.apache.spark.sql.Row): Int = {
        return 1
    }

    def doMatch2(cass:com.datastax.spark.connector.rdd.CassandraTableScanRDD[CassandraRow]): Int = {
        cass.first
        return 1
    }
}

/** Computes an approximation to pi */
object App{
  def main(args: Array[String]) {
    val conf = new SparkConf(true).setAppName("test cassandra")
    .set("spark.cassandra.connection.timeout_ms", "600000")
    .set("spark.cassandra.read.timeout_ms", "600000")
    val sc = new SparkContext(conf)
    Context.sparkContext = sc
    Context.sqlContext = new SQLContext(sc)
    // Context.cassandraContext = new CassandraSQLContext(sc)
    // val cass = sc.cassandraTable("chinamarket", "daymarketdata")
    // val df = Context.cassandraContext.sql("SELECT * from chinamarket.daymarketquery100 limit 1000")
    val tmp = new DenseMatrix(3,2, Array(1.0,2,3,4,5,6))
    val tmp2 = pow(tmp,2)
    val tmp3 = sum(tmp2)
    println("==============")
    println(Stat.brownianCorrelation(tmp,tmp))
    // Step 1 - 读取所有Sample数据
    // val sampleIndexArray = df.collect()
    // val sampleArray = sampleIndexArray.map(row => {
    //     cass.select("open", "high", "low", "close")
    //     .where("sid = ?", row.getInt(row.fieldIndex("sid")))
    //     .where("datetime >= ?", row.getTimestamp(row.fieldIndex("datetime")))
    //     .limit(100)
    //     })
    // val tmp = sampleArray.map(Utils.CassandraTableScanRDD2DoubleArray)
    // println(tmp)
    // Step 2 - 分发任务，计算brownianCorrelation
    println("Done!")
    sc.stop()
  }
}
// scalastyle:on println
