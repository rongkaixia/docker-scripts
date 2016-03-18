# Spark
FROM rk/spark-base:1.6.1
MAINTAINER keystone rongkaixia@gmail.com

# Expose TCP ports 7077 8080
EXPOSE 7077 8080

ADD files /root/spark_master_files

CMD ["/root/spark_master_files/default_cmd"]
