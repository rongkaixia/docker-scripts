# Spark 1.6.1
# Version 1.0.0
#
FROM apache-hadoop-hdfs-precise:2.6.4

MAINTAINER keystone rongkaixia@gmail.com

ENV SCALA_VERSION 2.10.5
ENV SPARK_VERSION 1.6.1
ENV SCALA_HOME /opt/scala-$SCALA_VERSION
ENV SPARK_HOME /opt/spark-$SPARK_VERSION
ENV PATH $SPARK_HOME/bin:$SPARK_HOME/sbin:$SCALA_HOME/bin:$PATH

# Install Scala
# ADD http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.tgz /
ADD scala-$SCALA_VERSION.tgz /opt/
# RUN (cd / && gunzip < scala-$SCALA_VERSION.tgz)|(cd /opt && tar -xvf -)
# RUN rm /scala-$SCALA_VERSION.tgz

# Install Spark 
# ADD http://apache.fayea.com/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop1.tgz /
ADD spark-$SPARK_VERSION-bin-hadoop2.6.tgz /opt/
# RUN (cd / && gunzip < spark-$SPARK_VERSION-bin-hadoop1.tgz)|(cd /opt && tar -xvf -)
# RUN (ln -s /opt/spark-$SPARK_VERSION-bin-hadoop1 /opt/spark-$SPARK_VERSION && rm /spark-$SPARK_VERSION-bin-hadoop1.tgz)
RUN ln -s /opt/spark-$SPARK_VERSION-bin-hadoop2.6 /opt/spark-$SPARK_VERSION

# Add Shark config files and configure script
# ADD files /root/spark_files
