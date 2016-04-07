// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

trait DateColumnConf{
    protected var dateColumn: Option[String] = Some("datetime")
    def setDateColumn(d: String): this.type = {
        dateColumn = Some(d)
        this
    }
    def getDateColumn(): String = {
        dateColumn.getOrElse("[not specified]")
    }
}

trait SidColumnConf{
    protected var sidColumn: Option[String] = Some("sid")
    def setSidColumn(d: String): this.type = {
        sidColumn = Some(d)
        this
    }
    def getSidColumn(): String = {
        sidColumn.getOrElse("[not specified]")
    }
}

trait MatchColumnsConf {
    protected var matchColumns: mutable.ListBuffer[String] = mutable.ListBuffer[String]()
    def setMatchColumns(cols: String*): this.type = {
        matchColumns.clear
        matchColumns.append(cols: _*)
        // cols.foreach(col => columns.append(col))
        this
    }
    def getMatchColumns(): List[String] = {
        matchColumns.toList
    }
}

trait DefaultOHLCMatchColumnsConf extends MatchColumnsConf{
    matchColumns = mutable.ListBuffer[String]("open", "high", "low", "close")
}

trait CassandraClusterProperty {
    protected var host : Option[String] = Some("localhost")
    protected var port : Option[Int] = Some(9042)
    protected var keyspace: Option[String] = None
    protected var tablename: Option[String] = None
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
    def getHost(): String = {
        host.get
    }
    def getPort(): Int = {
        port.get
    }
    def getKeyspace(): String = {
        keyspace.getOrElse("[not specified]")
    }
    def getTablename(): String = {
        tablename.getOrElse("[not specified]")
    }
}

class OHLCSearchEngineConf extends CassandraClusterProperty 
with DateColumnConf 
with SidColumnConf 
with DefaultOHLCMatchColumnsConf{
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

