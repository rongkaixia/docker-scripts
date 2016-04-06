// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
import scala.collection.mutable.{Set, ListBuffer}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector._

import com.keystone.cassandra.CassandraClient
import com.keystone.OHLCSearchEngine.{OHLCSearchEngineConf,Stat}

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}

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

@serializable
class SamplePeriod(sid_ :Int, beginTime_ :java.util.Date, endTime_ :java.util.Date, period_ :Int){
    var sid: Int = sid_
    var beginTime: java.util.Date = beginTime_
    var endTime: java.util.Date = endTime_
    var period: Int = period_
    // var exchangecd: String = _
}

class OHLCSearchEngine(
    @transient val sc: SparkContext,
    val conf: OHLCSearchEngineConf){

    private val _sc = sc
    private val _conf = conf

    def _checkConfigure(){

    }

    def similarity(){
        val cass = sc.cassandraTable("chinamarket", "daymarketdata")
        val selectColumns = _conf
    }

    def topN(n: Int){

    }

    def test(){

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

    // def doMatch2(query: SamplePeriod, cassandraHost: String, cassandraPort: Int): Double = {
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

    // def doMatch3(queries: Iterator[SamplePeriod], cassandraHost: String, cassandraPort: Int): Iterator[Double] = {
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

    def doMatch4(param: (Int, Seq[com.datastax.spark.connector.CassandraRow]), sidTimeFilterBroadcast: Seq[SamplePeriod]): Double = {
        // filter
        var ret = 0.0
        var sidTimeFilter = sidTimeFilterBroadcast.filter(_.sid == param._1)
        sidTimeFilter.foreach( SamplePeriod => {
            // filter data
            var data = param._2.filter(_.getDate("datetime").compareTo(SamplePeriod.beginTime) >= 0).filter(_.getDate("datetime").compareTo(SamplePeriod.endTime) <=0)
            var matrix = Utils.seqCassandraRow2DoubleDenseMatrix(data)
            var corr = Stat.brownianCorrelation(matrix, matrix)
            ret += 1.0
        })
        return ret
    }
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
    val SamplePeriodArray = df.collect()
    val sampleArray = SamplePeriodArray.map(row => {
        cass.select("open", "high", "low", "close")
        .where("sid = ?", row.getInt(row.fieldIndex("sid")))
        .where("datetime >= ?", row.getTimestamp(row.fieldIndex("datetime")))
        .limit(10)
        })
    // val sampleArray = SamplePeriodArray.map(row => {cass.select("open", "high", "low", "close").where("sid = ?", row.getInt(row.fieldIndex("sid"))).where("datetime >= ?", row.getTimestamp(row.fieldIndex("datetime"))).limit(10)})
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
    var tmp = rddRows.map(row => {new SamplePeriod(row.getInt(row.fieldIndex("sid")), row.getTimestamp(row.fieldIndex("begintime")), row.getTimestamp(row.fieldIndex("endtime")), period)})
    // .map(SearchEngine.doMatch2(_, cassandraHost, cassandraPort))
    // .mapPartitions(SearchEngine.doMatch3(_, cassandraHost, cassandraPort))

    // Step 2 - construct sharedVariable and query string
    var sidTimeFilter: Seq[SamplePeriod] = tmp.collect.toSeq
    val broadcastVar = sc.broadcast(sidTimeFilter)
    var sids = Set[Int]()
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
