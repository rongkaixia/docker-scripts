// scalastyle:off println
package com.mycompany.app

import scala.math.random
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

import scala.collection.JavaConversions._
import com.datastax.driver.core.ResultSet
import com.keystone.cassandra.CassandraClient

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

object Utils{
    def seqCassandraRow2DoubleDenseMatrix(data: Seq[CassandraRow]): DenseMatrix[Double] = {
        val nRows = data.size
        val nCols = 4
        var ret = new DenseMatrix[Double](nRows, nCols)
        var rowIndex = 0
        data.foreach( row => {
            ret(rowIndex, 0) = row.getDouble("open")
            ret(rowIndex, 1) = row.getDouble("high")
            ret(rowIndex, 2) = row.getDouble("low")
            ret(rowIndex, 3) = row.getDouble("close")
            // (0 to nCols - 1).foreach(colIndex => {ret(rowIndex,colIndex) = row.getDouble(colIndex)})
            rowIndex += 1
            })
        return ret
    }

    def cassandraResultSet2DoubleDenseMatrix(data: ResultSet): DenseMatrix[Double] = {
        val listRows = data.all.toList
        val nRows = listRows.size
        val nCols = listRows(0).getColumnDefinitions.size
        var ret = new DenseMatrix[Double](nRows, nCols)
        var rowIndex = 0
        listRows.map( row => {
            (0 to nCols - 1).foreach(colIndex => {ret(rowIndex,colIndex) = row.getDouble(colIndex)})
            rowIndex += 1
            })
        return ret
    }

    def cassandraTableScanRDD2DoubleArray(table: com.datastax.spark.connector.rdd.CassandraTableScanRDD[CassandraRow], rowSize: Int): DenseMatrix[Double] = {
        var tmp = table.map(row => {
                var ret = new Array[Double](row.size)
                var i = 0
                for(i <- 0 to row.size - 1){
                    ret(i) = row.getDouble(i)
                }
                ret
            }).reduce((a: Array[Double],b: Array[Double]) => a ++ b)
        return new DenseMatrix[Double](tmp.size/rowSize, rowSize, tmp)
    }
}

object SearchEngine{
    // def _row2DoubleArray(row: org.apache.spark.sql.Row){
    //     var ret = new Array[Double](row.size)
    // }
    def doMatch(A: DenseMatrix[Double], B: DenseMatrix[Double]): Double = {
        var ret = Stat.brownianCorrelation(A,B)
        return ret
    }

    // def doMatch2(query: SampleIndex, cassandraHost: String, cassandraPort: Int): Double = {
    //     var client = new CassandraClient
    //     client.connect(cassandraHost, cassandraPort)
    //     val queryString = "SELECT open, high, low, close FROM chinamarket.daymarketdata where " +
    //         "sid = " + query.sid + " AND datetime >= '" + query.datetime + 
    //         "' LIMIT " + query.period
    //     var result = client.execute(queryString)
    //     // var queryMatrix = Utils.cassandraResultSet2DoubleDenseMatrix(result)
    //     // client.close()
    //     return 1.0
    //     // return Stat.brownianCorrelation(queryMatrix, queryMatrix)
    // }

    // def doMatch3(queries: Iterator[SampleIndex], cassandraHost: String, cassandraPort: Int): Iterator[Double] = {
    //     var client = new CassandraClient
    //     client.connect(cassandraHost, cassandraPort)
    //     var n = queries.size
    //     var ret = queries.map( query => {
    //         val queryString = "SELECT open, high, low, close FROM chinamarket.daymarketdata where " +
    //             "sid = " + query.sid + " AND datetime >= '" + query.datetime + 
    //             "' LIMIT " + query.period
    //         var result = client.execute(queryString)
    //         var queryMatrix = Utils.cassandraResultSet2DoubleDenseMatrix(result)
    //         1.0
    //         // return 1.0
    //     }).collect{
    //         case x: Double => x
    //     }
    //     return ret
    //     // client.close()
    //     // return Stat.brownianCorrelation(queryMatrix, queryMatrix)
    // }

    def doMatch4(param: (Int, Seq[com.datastax.spark.connector.CassandraRow]), sidTimeFilterBroadcast: Seq[SampleIndex]): Double = {
        // filter
        var ret = 0.0
        var sidTimeFilter = sidTimeFilterBroadcast.filter(_.sid == param._1)
        sidTimeFilter.foreach( sampleIndex => {
            // filter data
            var data = param._2.filter(_.getDate("datetime").compareTo(sampleIndex.beginTime) >= 0).filter(_.getDate("datetime").compareTo(sampleIndex.endTime) <=0)
            var matrix = Utils.seqCassandraRow2DoubleDenseMatrix(data)
            // var corr = Stat.brownianCorrelation(matrix, matrix)
            ret += 1.0
        })
        return ret
    }
}

@serializable
class SampleIndex(sid_ :Int, beginTime_ :java.util.Date, endTime_ :java.util.Date, period_ :Int){
    var sid: Int = sid_
    var beginTime: java.util.Date = beginTime_
    var endTime: java.util.Date = endTime_
    var period: Int = period_
    // var exchangecd: String = _
}

object Context{
    var sparkContext: SparkContext = _

    var sqlContext: SQLContext = _

    var cassandraContext: CassandraSQLContext = _
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
    Context.cassandraContext = new CassandraSQLContext(sc)
    val cass = sc.cassandraTable("chinamarket", "daymarketdata")
    /* 旧代码
    // Step 1 - 读取所有待匹配的Sample数据
    val df = Context.cassandraContext.sql("SELECT * from chinamarket.daymarketquery100 limit 200")
    val sampleIndexArray = df.collect()
    val sampleArray = sampleIndexArray.map(row => {
        cass.select("open", "high", "low", "close")
        .where("sid = ?", row.getInt(row.fieldIndex("sid")))
        .where("datetime >= ?", row.getTimestamp(row.fieldIndex("datetime")))
        .limit(10)
        })
    // val sampleArray = sampleIndexArray.map(row => {cass.select("open", "high", "low", "close").where("sid = ?", row.getInt(row.fieldIndex("sid"))).where("datetime >= ?", row.getTimestamp(row.fieldIndex("datetime"))).limit(10)})
    val tmp = sampleArray.map(Utils.cassandraTableScanRDD2DoubleArray(_, 4))
    // Step 2 - 分发任务，计算brownianCorrelation
    val query = tmp(0) // 用第一个作为query样本，剩下的为数据集
    val smapleRDD = sc.parallelize(tmp.slice(1,tmp.size))
    val result = smapleRDD.map(SearchEngine.doMatch(query, _)).collect()
    result.foreach(println)
    */
   
    // 测试Slave从Cassandra中获取数据
    // Step 1 - read query
    val cassandraHost = conf.get("spark.cassandra.connection.host")
    val cassandraPort = conf.getInt("spark.cassandra.connection.port", 9042)
    val period = 10
    val rddRows = Context.cassandraContext.sql("SELECT * from chinamarket.daymarketquery11 limit 1000")
    var tmp = rddRows.map(row => {new SampleIndex(row.getInt(row.fieldIndex("sid")), row.getTimestamp(row.fieldIndex("begintime")), row.getTimestamp(row.fieldIndex("endtime")), period)})
    // .map(SearchEngine.doMatch2(_, cassandraHost, cassandraPort))
    // .mapPartitions(SearchEngine.doMatch3(_, cassandraHost, cassandraPort))

    // Step 2 - construct sharedVariable and query string
    var sidTimeFilter: Seq[SampleIndex] = tmp.collect.toSeq
    val broadcastVar = sc.broadcast(sidTimeFilter)
    var sids = mutable.Set[Int]()
    sidTimeFilter.foreach(ele => sids.update(ele.sid, true))
    var queryString = "sid in ("
    var hasValue = false
    sids.foreach(key => {
        if(hasValue){
            queryString += ","
            queryString += key
        }else{
            queryString += key
            hasValue = true
        }
    })
    queryString += ")"

    // Step 3 - read data
    var data = cass.select("sid", "datetime", "open", "high", "low", "close").where(queryString).keyBy(row => row.getInt("sid")).spanByKey

    // Step 4 - run algorithm
    data.map(SearchEngine.doMatch4(_, broadcastVar.value)).toArray.foreach(println)
    println("Done!")
    sc.stop()
  } 
}
// scalastyle:on println
