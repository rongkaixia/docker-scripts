# -*- coding: utf-8 -*-
###
### import wind data into cassandra cluster
###
from __future__ import print_function
import sys
import time
import os
import pandas as pd
from datetime import datetime
import argparse
import dateutil
from os import listdir
from os.path import isfile, join, splitext

from cassandra.cluster import Cluster

# from keystone.utils.json_utils import json_clean

default_sys_stdout = sys.stdout
default_sys_stderr = sys.stderr

CASSANDRA_KEYSPACE = "chinamarket"

class ArgumentParserError(Exception): pass

class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)

# Initialize parser
def init_command_parser():
	parser = ThrowingArgumentParser()

	parser.add_argument('--data-path', type=str, help="path of data", required=True)
	parser.add_argument('--dt', type=str, help='datetime column', default="datetime")
	parser.add_argument('--dt-format', type=str, help='datetime format', default="%Y-%m-%d %H:%M:%S")
	parser.add_argument('--delimiter', type=str, help='price column', default=',')


	# subparsers = parser.add_subparsers(title='subcommands',
	# 	description='valid subcommands', 
	# 	help='additional help', 
	# 	dest='subparser_name')

	# parser_create = subparsers.add_parser(CREATE_COMMAND, help='create an empty kstable')
	# parser_create.add_argument('--path', 
	# 	type=str, 
	# 	help='create an empty kstable in the given path', 
	# 	required=True)

	# parser_preview = subparsers.add_parser(PREVIEW_COMMAND, help='preview DataFrame in kstable')
	# parser_preview.add_argument('--path', 
	# 	type=str, 
	# 	help='get dataframe from the given kstable', 
	# 	required=True)
	# parser_preview.add_argument('-n', type=int, help='number of rows')

	# parser_union = subparsers.add_parser(UNION_COMMAND, help='union csv/excel source with kstable')
	# parser_union.add_argument('--path', type=str, help='path of kstable', required=True)
	# parser_union.add_argument('--file', type=str, help='path of csv/excel file', required=True)
	# parser_union.add_argument('--type', type=str, help='type, csv or excel', default="csv")
	# parser_union.add_argument('--dt', type=str, help='datetime column', default="datetime")
	# parser_union.add_argument('--dt-format', type=str, help='datetime format', default="%Y-%m-%d %H:%M:%S")
	# parser_union.add_argument('--sid', type=str, help='security column')
	# parser_union.add_argument('--price', type=str, help='price column')
	# parser_union.add_argument('--delimiter', type=str, help='price column', default=',')

	# parser_astype = subparsers.add_parser(ASTYPE_COMMAND, help='change column type of DataFrame')
	# parser_astype.add_argument('--path', type=str, help='path of kstable', required=True)
	# parser_astype.add_argument('--column', type=str, help='column name', required=True)
	# parser_astype.add_argument('--type', choices=['int', 'str', 'float'], help='type', required=True)

	return parser

def read_csv(file, args):
	df = pd.read_csv(file, skip_blank_lines=True, sep=args.delimiter)

	# parse datetime, strptime is fast
	try:
		dt_column = [datetime.strptime(x, args.dt_format) for x in df[args.dt]]
	except Exception as e:
		# try dateutil.parser
		try:
			dt_column = [dateutil.parser.parse(x) for x in df[args.dt]]
		except Exception as e:
			raise ValueError("cannot parse datetime column '" + args.dt + "'")
	df[args.dt] = dt_column

	# get sid and exchangeCD
	name = os.path.basename(file)
	l = name.split('.')
	sid = int(l[0])
	exchangeCD = l[1]

	df['sid'] = sid
	df['exchangeCD'] = exchangeCD

	# rename column
	df.rename(columns={
		args.dt: "datetime", 
		"PRE_CLOSE": "pre_close",
		"OPEN": "open",
		"HIGH": "high",
		"LOW": "low",
		"CLOSE": "close",
		"VOLUME": "volume"}, inplace=True)
	return df

def save_to_cassandra(session, df):
	for (index, row) in df.iterrows():
		session.execute(
	    """
	    INSERT INTO daymarketdata100 (sid, exchangeCD, datetime, open, high, low, close, pre_close, volume)
	    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	    """,
	    (row.sid, row.exchangeCD, row.datetime.to_datetime(), row.open, row.high, row.low, row.close, row.pre_close, row.volume)
		)
	# session.execute(
	# 	"""
	# 	CREATE TABLE DayMarketData (
	# 	  sid int,
	# 	  exchangeCD text,
	# 	  datetime timestamp,
	# 	  open double,
	# 	  high double,
	# 	  low double,
	# 	  close double,
	# 	  pre_close double,
	# 	  volume bigint,
	# 	  PRIMARY KEY (sid, exchangeCD)
	# 	)
	# 	"""
	# 	)

def run(args):
	cluster = Cluster()
	session = cluster.connect(CASSANDRA_KEYSPACE)

	csvfiles = [f for f in listdir(args.data_path) if splitext(f)[-1] == ".csv" and isfile(join(args.data_path, f))]
	for file in csvfiles:
		print("processing %s"%(file))
		df = read_csv(join(args.data_path, file), args)
		save_to_cassandra(session, df)

if __name__ == "__main__":
	parser = init_command_parser()
	args = parser.parse_args()
	run(args)
	print("done")