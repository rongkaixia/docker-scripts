// scalastyle:off println
package com.mycompany.app

import org.joda.time.DateTime
import java.text.SimpleDateFormat
import scala.math.{random, pow}

import scala.collection.JavaConversions._
import com.datastax.driver.core.{ResultSet, BoundStatement}
import com.keystone.cassandra.CassandraClient

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}

import com.keystone.OHLCSearchEngine.{OHLCSearchEngineConf, Stat}
object Timer{
   def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / pow(10,6) + "ms")
        result
    } 
}

/** Computes an approximation to pi */
object TestCassandra{
  def main(args: Array[String]) {
    
    var cassandraHost = "172.17.0.3"
    var cassandraPort = 9042
    var client = new CassandraClient
    var tmp = Timer.time{
        println("================connecting Cassandra==============")
        client.connect(cassandraHost, cassandraPort)   
    }
    var sampleIndex = client.execute("SELECT * from chinamarket.daymarketquery10 limit " + args(0))
    val listRows = sampleIndex.all.toList
    // var dt = DateTime.parse("1996-04-09")
    
    val queryString = "SELECT open, high, low, close FROM chinamarket.daymarketdata100 where sid = ? " + "and datetime >= ? " + "and datetime <= ? "
    var statement = client.getSession().prepare(queryString)
    var result = Timer.time{
        println("==============start query==========")
        // var count = 1
        listRows.map( row => {
            // println("quering " + count)
            // count += 1
            var boundStatement = new BoundStatement(statement).setInt(0, row.getInt("sid")).setTimestamp(1, row.getTimestamp("begintime")).setTimestamp(2, row.getTimestamp("endtime"))
            var data = client.getSession().execute(boundStatement).all
            // var queryMatrix = Utils.cassandraResultSet2DoubleDenseMatrix(data)
            // var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            // var queryString = "SELECT datetime,open, high, low, close FROM chinamarket.daymarketdata where sid = " + row.getInt("sid") + " AND datetime > '" + sdf.format(row.getTimestamp("datetime")) + "' LIMIT 10"
            // var data = client.execute(queryString)
        })
        // client.close
        println("============end query=================")
        // println(data)
        // var queryMatrix = Utils.cassandraResultSet2DoubleDenseMatrix(data)
        // println(queryMatrix)
    }

  }
}
// scalastyle:on println
