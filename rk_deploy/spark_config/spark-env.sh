#!/usr/bin/env bash
export SCALA_HOME=/opt/scala-2.10.3
export SPARK_HOME=__SPARK_HOME__
export SPARK_WORKER_CORES=4
export SPARK_MEM=5000m
export SPARK_WORKER_MEMORY=3000m
export SPARK_MASTER_MEM=1000m
export SPARK_MASTER_IP=__MASTER__
export HADOOP_HOME="/etc/hadoop"
export MASTER="spark://__MASTER__:7077"
export SPARK_LOCAL_DIR=/tmp/spark
#SPARK_JAVA_OPTS="-Dspark.local.dir=/tmp/spark "
#SPARK_JAVA_OPTS+=" -Dspark.akka.logLifecycleEvents=true "
#SPARK_JAVA_OPTS+="-Dspark.kryoserializer.buffer.mb=10 "
#SPARK_JAVA_OPTS+="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "
#export SPARK_JAVA_OPTS
#SPARK_DAEMON_JAVA_OPTS+=" -Dspark.akka.logLifecycleEvents=true "
#export SPARK_DAEMON_JAVA_OPTS
export JAVA_HOME=__JAVA_HOME__

# Instead of using a random port, bind the worker to a specific port
export SPARK_WORKER_PORT=8888
