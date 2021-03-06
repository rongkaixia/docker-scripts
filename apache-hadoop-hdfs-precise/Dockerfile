# Base Ubuntu Precise 12.04 LTS image
#
FROM ubuntu:precise
MAINTAINER keystone rongkaixia@gmail.com

# Setup a volume for data
VOLUME ["/data"]

# Env
ENV HADOOP_VERSION 2.6.4
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION
ENV PATH $HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Set correct source list
RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
RUN echo "deb http://archive.ubuntu.com/ubuntu precise-updates main universe" >> /etc/apt/sources.list

# install add-apt-repository
RUN apt-get update && apt-get install -y python-software-properties

# install a few other useful packages plus Open Jdk 8
# RUN add-apt-repository ppa:webupd8team/java
RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update && apt-get upgrade -y && apt-get install -y less net-tools vim-tiny sudo openssh-server iputils-ping openjdk-8-jdk

# Install Hadoop
ADD hadoop-2.6.4.tar.gz /opt/

# Docker messes up /etc/hosts and adds two entries for 127.0.0.1
# we try to recover from that by giving /etc/resolv.conf and therefore
# the nameserver priority
RUN sed -i s/"files dns"/"dns files"/ /etc/nsswitch.conf

# add Hadoop config file templates
ADD files /root/hadoop_files

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
