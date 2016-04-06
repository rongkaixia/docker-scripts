// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
import scala.collection.mutable.{Set, ListBuffer}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

trait DateColumnConf{
    var dateColumn: Option[String] = Some("date")
    def setDateColumn(d: String): this.type = {
        dateColumn = Some(d)
        this
    }
}

trait SidColumnConf{
    var sidColumn: Option[String] = Some("sid")
    def setSidColumn(d: String): this.type = {
        sidColumn = Some(d)
        this
    }
}

trait MatchColumnsConf {
    var matchColumns: ListBuffer[String] = ListBuffer[String]()
    def setMatchColumns(cols: String*): this.type = {
        matchColumns.clear
        matchColumns.append(cols: _*)
        // cols.foreach(col => columns.append(col))
        this
    }
}

trait DefaultOHLCMatchColumnsConf extends MatchColumnsConf{
    matchColumns = ListBuffer[String]("open", "high", "low", "close")
}

trait CassandraClusterProperty {
    var host : Option[String] = Some("localhost")
    var port : Option[Int] = Some(9042)
    var keyspace: Option[String] = None
    var tablename: Option[String] = None
    def setHost(s: String): this.type = {
        host = Some(s)
        this
    }
    def setPort(p: Int): this.type = {
        port = Some(p)
        this
    }
    def setKeyspace(k: String): this.type = {
        keyspace = Some(k)
        this
    }
    def setTable(k: String): this.type = {
        tablename = Some(k)
        this
    }
}

class OHLCSearchEngineConf extends CassandraClusterProperty with DateColumnConf with SidColumnConf with DefaultOHLCMatchColumnsConf{
    def print(){
        println("cassandraHost: " + host.get + ":" + port.get)
        println("table: " + keyspace.getOrElse("[not specified]") + ":" + tablename.getOrElse("[not specified]"))
        println("dateColumn: " + dateColumn.getOrElse("[not specified]"))
        println("sidColumn: " + sidColumn.getOrElse("[not specified]"))
        println("matchColumns: " + matchColumns.reduce((col1, col2) => {col1 + "," + col2}))
    }
}

object OHLCSearchEngineConf{
    def apply(){
        new OHLCSearchEngineConf()
    }
}

