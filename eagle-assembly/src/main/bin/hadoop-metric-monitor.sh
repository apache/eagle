##!/bin/bash
#
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
##    http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
#
source $(dirname $0)/eagle-env.sh

######################################################################
##            Create Kafka topic for HADOOP METRIC
######################################################################


######################################################################
##            Import stream metadata for HADOOP METRIC
######################################################################
./hadoop-metric-init.sh

######################################################################
##            Run topology for HADOOP METRIC
######################################################################
./eagle-topology.sh --main org.apache.eagle.hadoop.metric.HadoopJmxMetricMonitor --topology sandbox-hadoopjmx-topology --config ../conf/sandbox-hadoopjmx-topology.conf start

######################################################################
##            Setup minutely crontab job for HADOOP METRIC
######################################################################
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cp $DIR/../tools/hadoop_jmx_collector/config-sample.json $DIR/../tools/hadoop_jmx_collector/config.json
command="python $DIR/../tools/hadoop_jmx_collector/hadoop_jmx_kafka.py"
echo $job
job="* * * * * $command >> $DIR/../logs/hadoop_metric.log"
cat <(fgrep -i -v "$command" <(crontab -l)) <(echo "$job") | crontab -