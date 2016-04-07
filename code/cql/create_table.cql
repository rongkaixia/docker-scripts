
DROP KEYSPACE IF EXISTS ChinaMarket;

CREATE KEYSPACE ChinaMarket
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE ChinaMarket;

CREATE TABLE DayMarketData10 (
  sid int,
  exchangeCD text,
  datetime timestamp,
  open double,
  high double,
  low double,
  close double,
  pre_close double,
  volume double,
  PRIMARY KEY (sid, datetime, exchangeCD)
);

/*
CREATE TABLE DayMarketData (
  sid int,
  exchangeCD text,
  datetime timestamp,
  open double,
  high double,
  low double,
  close double,
  pre_close double,
  volume double,
  PRIMARY KEY (sid, datetime, exchangeCD)
);
*/

CREATE TABLE DayMarketData100 (
  sid int,
  exchangeCD text,
  datetime timestamp,
  open double,
  high double,
  low double,
  close double,
  pre_close double,
  volume double,
  PRIMARY KEY (sid, datetime, exchangeCD)
);

CREATE TABLE DayMarketQuery11 (
    sid int,
    begintime timestamp,
    endtime timestamp,
    period int,
    PRIMARY KEY (sid, begintime, endtime)
);

/* COPY DayMarketData from '../cassandra-data/daymarketdata'; */
COPY DayMarketData10 from '../cassandra-data/daymarketdata10';
COPY DayMarketData100 from '../cassandra-data/daymarketdata100';
COPY DayMarketQuery11 from '../cassandra-data/daymarketquery11-100';