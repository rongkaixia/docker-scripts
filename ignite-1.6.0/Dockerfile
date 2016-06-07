# Spark 1.6.1
# Version 1.0.0
#
FROM spark-base:latest

MAINTAINER keystone rongkaixia@gmail.com

ENV IGNITE_VERSION 1.6.0
ENV IGNITE_HOME /opt/ignite-$IGNITE_VERSION

ENV SCALA_VERSION 2.10.5
ENV SPARK_VERSION 1.6.1
ENV SCALA_HOME /opt/scala-$SCALA_VERSION
ENV SPARK_HOME /opt/spark-$SPARK_VERSION
ENV PATH $SPARK_HOME/bin:$SCALA_HOME/bin:$IGNITE_HOME/bin:$PATH

RUN apt-get install zip unzip

# Add Shark config files and configure script
ADD apache-ignite-fabric-1.6.0-bin.zip /opt/

RUN (cd /opt && unzip ./apache-ignite-fabric-1.6.0-bin.zip)
RUN (ln -s /opt/apache-ignite-fabric-1.6.0-bin /opt/ignite-$IGNITE_VERSION && rm /opt/apache-ignite-fabric-1.6.0-bin.zip)


