FROM sequenceiq/ambari:1.7.0

MAINTAINER Zqin

ENV EAGLE_DOWNLOAD_LINK http://66.211.190.194/eagle-0.1.0.tar.gz

RUN curl -sL $EAGLE_DOWNLOAD_LINK | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s ./eagle-0.1.0 eagle
ENV EAGLE_HOME=/usr/local/eagle


RUN yum install -y httpd ganglia ganglia-gmetad ganglia-gmond ganglia-web nagios kafka zookeeper storm hbase tez hadoop snappy snappy-devel hadoop-libhdfs ambari-log4j hive hive-hcatalog hive-webhcat webhcat-tar-hive webhcat-tar-pig mysql-connector-java mysql-server

ADD serf /usr/local/serf

RUN mkdir -p /var/log/httpd

ADD install-cluster.sh /tmp/
ADD eagle-singlenode.json /tmp/
ADD eagle-multinode.json /tmp/
ADD wait-for-eagle.sh /tmp/
ADD deploy-eagle.sh /usr/local/eagle/deploy.sh

EXPOSE 9099 8744
