
DROP KEYSPACE IF EXISTS ChinaMarket;

CREATE KEYSPACE ChinaMarket
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE ChinaMarket;

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


CREATE TABLE DayMarketQuery100 (
  sid int,
  exchangeCD text,
  datetime timestamp,
  period int,
  PRIMARY KEY (sid, datetime, exchangeCD)
);

COPY DayMarketData from '../cassandra-data/daymarketdata';
COPY DayMarketQuery100 from '../cassandra-data/daymarketquery100';