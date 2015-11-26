# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


FROM sequenceiq/ambari:1.7.0

MAINTAINER dev@eagle.incubator.apache.org

ENV EAGLE_DOWNLOAD_LINK http://66.211.190.194/eagle-0.1.0.tar.gz

RUN curl -sL $EAGLE_DOWNLOAD_LINK | tar -xz -C /usr/hdp/current
RUN cd /usr/hdp/current && ln -s ./eagle-0.1.0 eagle
ENV EAGLE_HOME=/usr/hdp/current/eagle


RUN yum install -y httpd ganglia ganglia-gmetad ganglia-gmond ganglia-web nagios kafka zookeeper storm hbase tez hadoop snappy snappy-devel hadoop-libhdfs ambari-log4j hive hive-hcatalog hive-webhcat webhcat-tar-hive webhcat-tar-pig mysql-connector-java mysql-server

ADD serf /usr/local/serf

ADD httpd /var/log/httpd

ADD install-cluster.sh /tmp/
ADD eagle-singlenode.json /tmp/
ADD eagle-multinode.json /tmp/
ADD wait-for-eagle.sh /tmp/
ADD deploy-eagle.sh /usr/hdp/current/eagle/deploy.sh

EXPOSE 9099 8744
