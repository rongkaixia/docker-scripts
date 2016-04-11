#!/bin/bash
JAR_FILE=${@: -1}
cp $JAR_FILE /tmp/
if [ $? -eq 0 ];then
	JAR_FILE_NAME=$(basename $JAR_FILE)
	echo "sudo -u hdfs /opt/spark-1.6.1-bin-hadoop1/bin/spark-submit --class com.keystone.OHLCSearchEngine.Test --master spark://master:7077 --conf spark.cassandra.connection.host=cassandra --conf spark.executor.memory=2000m /tmp/$JAR_FILE_NAME"
	sudo -u hdfs /opt/spark-1.6.1-bin-hadoop1/bin/spark-submit --class com.keystone.OHLCSearchEngine.Test --master spark://master:7077 --conf spark.cassandra.connection.host=cassandra --conf spark.executor.memory=1000m /tmp/$JAR_FILE_NAME
fi
