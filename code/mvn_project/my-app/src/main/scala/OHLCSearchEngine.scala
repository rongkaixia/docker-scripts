// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
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

// TODO: 需要refactor
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
}

@serializable
class SamplePeriod(sid_ :Int, beginTime_ :java.util.Date, endTime_ :java.util.Date, period_ :Int){
    var sid: Int = sid_
    var beginTime: java.util.Date = beginTime_
    var endTime: java.util.Date = endTime_
    var period: Int = period_
    // var exchangecd: String = _
}

// TODO: 需要refactor
object WorkerFunction{
    def apply(param: (Int, Seq[com.datastax.spark.connector.CassandraRow]), samples: List[SamplePeriod]): Double = {
        var ret = 0.0
        var samplesWithSid = samples.filter(_.sid == param._1)
        samplesWithSid.foreach( sample => {
            // filter data
            var sampleData = param._2.filter(_.getDate("datetime").compareTo(sample.beginTime) >= 0).filter(_.getDate("datetime").compareTo(sample.endTime) <=0)
            var matrix = Utils.seqCassandraRow2DoubleDenseMatrix(sampleData)
            var corr = Stat.brownianCorrelation(matrix, matrix)
            ret += 1.0
        })
        ret
    }
}

class OHLCSearchEngine(
    @transient val sc: SparkContext,
    val conf: OHLCSearchEngineConf){

    private val _sc = sc
    private val _conf = conf
    private val _table = sc.cassandraTable(_conf.getKeyspace, _conf.getTablename)

    def _checkConfigure(){

    }

    def similarity(samples: List[SamplePeriod]): Array[Double] = {
        // filter data and group sid
        val matchColumns = _conf.getMatchColumns
        val selectColumns = _conf.getDateColumn :: (_conf.getSidColumn :: _conf.getMatchColumns)
        val selectColumnsRef = selectColumns.map(col => new ColumnName(col))
        val filterString = "sid in (" + samples.map(ele => ele.sid).distinct.map(ele => ele.toString).reduce((a,b) => a + "," + b) + ")"
        val data = _table.select(selectColumnsRef: _*).where(filterString).keyBy(row => row.getInt("sid")).spanByKey

        // broadcast variable
        val broadcastVar = _sc.broadcast(samples)

        // worker function
        // def workerFunc(param: (Int, Seq[com.datastax.spark.connector.CassandraRow])): Double = {
        //     var ret = 0.0
        //     var samplesWithSid = broadcastVar.value.filter(_.sid == param._1)
        //     samplesWithSid.foreach( sample => {
        //         // filter data
        //         var sampleData = param._2.filter(_.getDate("datetime").compareTo(sample.beginTime) >= 0).filter(_.getDate("datetime").compareTo(sample.endTime) <=0)
        //         var matrix = Utils.seqCassandraRow2DoubleDenseMatrix(sampleData)
        //         var corr = Stat.brownianCorrelation(matrix, matrix)
        //         ret += 1.0
        //     })
        //     ret
        // }

        // run and collect result
        data.map(WorkerFunction.apply(_, broadcastVar.value)).toArray
    }

    def topN(n: Int){

    }
}

/** Computes an approximation to pi */
object Test{
  def main(args: Array[String]) {
    // initilize sc
    val sparkConf = new SparkConf(true).setAppName("test cassandra")
    .set("spark.cassandra.connection.timeout_ms", "600000")
    .set("spark.cassandra.read.timeout_ms", "600000")
    val sc = new SparkContext(sparkConf)

    // initilize engine
    val cassHost = sparkConf.get("spark.cassandra.connection.host")
    val cassPort = sparkConf.getInt("spark.cassandra.connection.port", 9042)
    val conf = new OHLCSearchEngineConf()
    .setHost(cassHost)
    .setPort(cassPort)
    .setKeyspace("chinamarket")
    .setTable("daymarketdata100")
    val engine = new OHLCSearchEngine(sc, conf)

    // read query
    val cc = new CassandraSQLContext(sc)
    val period = 10
    val rddRows = cc.sql("SELECT * from chinamarket.daymarketquery11 limit 10000")
    var tmp = rddRows.collect.map(row => {new SamplePeriod(row.getInt(row.fieldIndex("sid")), row.getTimestamp(row.fieldIndex("begintime")), row.getTimestamp(row.fieldIndex("endtime")), period)})
    val samples = tmp.toList

    // run
    engine.similarity(samples).foreach(println)
    println("Done!")
    sc.stop()
  } 
}
// scalastyle:on println
