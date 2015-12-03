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

# NOTICE: This docker image is built based sequenceiq/ambari which is open sourced under github: https://github.com/sequenceiq/docker-ambari
FROM sequenceiq/ambari:1.7.0

MAINTAINER dev@eagle.incubator.apache.org

# Eagle environment
ENV EAGLE_HOME=/usr/hdp/current/eagle

# Install Eagle Package
#
# [Option #1] Download eagle package from external link
# ENV EAGLE_DOWNLOAD_LINK http://66.211.190.194/eagle-0.1.0.tar.gz
# RUN curl -sL $EAGLE_DOWNLOAD_LINK | tar -xz -C /usr/hdp/current
# RUN cd /usr/hdp/current && ln -s ./eagle-0.1.0 eagle

# [Option #2] Load eagle package from locally built,
# In such way, should build with `eagle-docker.sh buid` instead of executing native
# `docker build` directly, to prepare the eagle package into build/eagle
ADD target/eagle-current /usr/hdp/current/eagle

# Load External Packages
RUN yum install -y httpd ganglia ganglia-gmetad ganglia-gmond ganglia-web nagios kafka zookeeper storm hbase tez hadoop snappy snappy-devel hadoop-libhdfs ambari-log4j hive hive-hcatalog hive-webhcat webhcat-tar-hive webhcat-tar-pig mysql-connector-java mysql-server

# Load Static Resources
ADD resource/serf /usr/local/serf
ADD resource/httpd /var/log/httpd
ADD resource/install-cluster.sh /tmp/
ADD resource/eagle-singlenode.json /tmp/
ADD resource/eagle-multinode.json /tmp/
ADD resource/wait-for-eagle.sh /tmp/
ADD resource/deploy-eagle.sh /usr/hdp/current/eagle/deploy.sh

EXPOSE 9099 8744 8080 2181 2888 6667 60020 60030 60010
