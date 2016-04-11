// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._
import com.datastax.driver.core.{ResultSet, BoundStatement, Row}

import com.keystone.cassandra.CassandraClient
import com.keystone.OHLCSearchEngine.{OHLCSearchEngineConf,Stat}

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}

object Utils{
    def seqCassandraRow2DoubleDenseMatrix(data: Seq[CassandraRow], columns: List[String]): DenseMatrix[Double] = {
        val nRows = data.size
        val nCols = columns.size
        val arrayData = data
        .map( row => { columns.map(col => row.getDouble(col)) })
        .flatten
        .toArray
        new DenseMatrix[Double](nCols, nRows, arrayData).t
    }

    def listDatastaxRow2DoubleDenseMatrix(data: List[Row], columns: List[String]): DenseMatrix[Double] = {
        val nRows = data.size
        val nCols = columns.size
        val arrayData = data
        .map( row => { columns.map(col => row.getDouble(col)) })
        .flatten
        .toArray
        new DenseMatrix[Double](nCols, nRows, arrayData).t
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
    private val _table = sc.cassandraTable(_conf.getKeyspace, _conf.getTablename)

    def similarity(pattern: DenseMatrix[Double], samples: List[SamplePeriod]): List[Double] = {
        val matchColumns = _conf.getMatchColumns
        val host = _conf.getHost
        val port = _conf.getPort

        // 构造query string，取K线相关的区间
        val queryString = "SELECT " + 
        matchColumns.reduce((a,b) => a + "," + b) + 
        " FROM " +
        _conf.getKeyspace + "." + _conf.getTablename +
        " WHERE " +
        _conf.getSidColumn + " = ? " +
        " AND " +
        _conf.getDateColumn + " >= ? " +
        " AND " +
        _conf.getDateColumn + " <= ? "

        // construct broadcast variable
        val broadcastPattern = _sc.broadcast(pattern)
        val broadcastQueryString = _sc.broadcast(queryString)

        // worker function
        val workerFunc: (Iterator[SamplePeriod]) => Iterator[Double] =
            (samplesPart: Iterator[SamplePeriod]) => {
                val client = new CassandraClient
                client.connect(host, port)
                val statement = client.getSession().prepare(broadcastQueryString.value)

                samplesPart.map( sample => {
                    val boundStatement = new BoundStatement(statement)
                    .setInt(0, sample.sid)    
                    .setTimestamp(1, sample.beginTime)
                    .setTimestamp(2, sample.endTime)
                    val matrix = Utils.listDatastaxRow2DoubleDenseMatrix(
                        client.getSession().execute(boundStatement).all.asScala.toList, 
                        matchColumns)
                    Stat.brownianCorrelation(broadcastPattern.value, matrix)
                })
            }

        _sc.parallelize(samples).mapPartitions(workerFunc, true).collect.toList
    }

    def similarityOld(pattern: DenseMatrix[Double], samples: List[SamplePeriod]): List[Double] = {
        // filter data and group sid
        val matchColumns = _conf.getMatchColumns
        val selectColumns = _conf.getDateColumn :: (_conf.getSidColumn :: _conf.getMatchColumns)
        val selectColumnsRef = selectColumns.map(col => new ColumnName(col))
        val filterString = 
            "sid in (" + 
            samples
            .map(ele => ele.sid)
            .distinct
            .map(ele => ele.toString)
            .reduce((a,b) => a + "," + b) + 
            ")"

        val data = _table
            .withReadConf(readConf)
            .select(selectColumnsRef: _*)
            .where(filterString)
            .keyBy(row => row.getInt("sid"))
            .spanByKey
            
        // construct broadcast variable
        val samplesWithIndex: Map[SamplePeriod, Int] = Array.tabulate(samples.length){ i => (samples(i), i) }.toMap
        val broadcastVar = _sc.broadcast(samplesWithIndex)
        val broadcastPattern = _sc.broadcast(pattern)

        // worker function
        // OUTPUT: (index:Int, similarity:Double) where index is
        // the original index in samples, since the worker processed
        // multiple samples with the same sid.
        val workerFunc: ((Int, Seq[com.datastax.spark.connector.CassandraRow])) => Map[Int, Double] = 
            (param: (Int, Seq[com.datastax.spark.connector.CassandraRow])) => {
                broadcastVar.value
                .filterKeys(_.sid == param._1) //filter sid, 只取与当前worker中的sid一样的samples
                .map( kv => {
                    // filter data, 取K线相关的区间
                    val sample = kv._1
                    val index = kv._2
                    val sampleData = param._2
                        .filter(_.getDate("datetime").compareTo(sample.beginTime) >= 0)
                        .filter(_.getDate("datetime").compareTo(sample.endTime) <=0)
                    val matrix = Utils.seqCassandraRow2DoubleDenseMatrix(sampleData, matchColumns)
                    (index, Stat.brownianCorrelation(broadcastPattern.value, matrix))
                })
                .toMap
            }

        // run and reconstruct result
        data.map(workerFunc)
        .reduce((a, b) => a ++ b)
        .toSeq
        .sortBy(_._1)
        .map(ele => ele._2)
        .toList
    }
}

/** Test OHLCSearchEngine */
object Test{
  def main(args: Array[String]) {
    val keyspace = "chinamarket"
    val table = "daymarketdata100"

    // initilize sc
    val sparkConf = new SparkConf(true)
    .set("spark.cassandra.connection.timeout_ms", "600000")
    .set("spark.cassandra.read.timeout_ms", "600000")
    .set("spark.cassandra.input.split.size_in_mb", "1")
    val sc = new SparkContext(sparkConf)

    // initilize engine
    val cassHost = sparkConf.get("spark.cassandra.connection.host")
    val cassPort = sparkConf.getInt("spark.cassandra.connection.port", 9042)
    val conf = new OHLCSearchEngineConf()
    .setHost(cassHost)
    .setPort(cassPort)
    .setKeyspace(keyspace)
    .setTable(table)
    val engine = new OHLCSearchEngine(sc, conf)

    // var rdd = sc.parallelize(0 to 1000000, 20)
    // rdd.map(ele => ele * 10).collect
    
    // =======构造pattern跟sample数据用于测试================
    val cc = new CassandraSQLContext(sc)
    val period = 10
    val rddRows = cc.sql("SELECT * from chinamarket.daymarketquery10 limit 10000")
    var tmp = rddRows
    .collect
    .map(row => {
        new SamplePeriod(
            row.getInt(row.fieldIndex("sid")), 
            row.getTimestamp(row.fieldIndex("begintime")), 
            row.getTimestamp(row.fieldIndex("endtime")), period)
        })
    .toList
    val pattern = tmp.head
    val samples = tmp.tail

    // get pattern open, high, low, close data
    val selectColumns = conf.getMatchColumns.map(col => new ColumnName(col))
    val patternData = sc
    .cassandraTable(keyspace, table)
    .select(selectColumns: _*)
    .where("sid = ?", pattern.sid)
    .where("datetime >= ?", pattern.beginTime)
    .where("datetime <= ?", pattern.endTime)
    .collect
    .toSeq
    val patternMatrix = Utils.seqCassandraRow2DoubleDenseMatrix(patternData, conf.getMatchColumns)

    // run
    println("===================CALCULATing SIMILARITY====================")
    val result = engine.similarity(patternMatrix, samples)
    val top10Index = Array
    .tabulate(result.length){ i => (result(i), i) }
    .toSeq
    .filter(!_._1.isNaN)
    .sortBy(_._1)
    .map(ele => ele._2)
    .reverse
    .take(10)

    // result.foreach(println)
    println("TOP 10 IS:")
    top10Index.foreach(i => println("similiraty: " + result(i) +"\tsample: [" + samples(i).sid + "," + samples(i).beginTime + "," + samples(i).endTime + "]"))
    println("Done!")
    
    sc.stop()
  } 
}
// scalastyle:on println
