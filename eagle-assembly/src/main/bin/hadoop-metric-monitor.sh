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
##            Import stream metadata for HADOOP METRIC
######################################################################
${EAGLE_HOME}/bin/hadoop-metric-init.sh

######################################################################
##            Run topology for HADOOP METRIC
######################################################################
### if topology exists, we should shutdown it
echo "check topology status ..."
active=$(${EAGLE_HOME}/bin/eagle-topology.sh --topology sandbox-hadoopjmx-topology status | grep ACTIVE)
echo "topology status $active"
if [ "$active" ]; then
 echo "stop topology ..."
 ${EAGLE_HOME}/bin/eagle-topology.sh --topology sandbox-hadoopjmx-topology stop
fi
echo "start Eagle Hadoop Metric Monitoring topology"
${EAGLE_HOME}/bin/eagle-topology.sh --main org.apache.eagle.hadoop.metric.HadoopJmxMetricMonitor --topology sandbox-hadoopjmx-topology --config ${EAGLE_HOME}/conf/sandbox-hadoopjmx-topology.conf start

######################################################################
##            Setup minutely crontab job for HADOOP METRIC
######################################################################
echo "set up crontab script"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cp $DIR/../tools/hadoop_jmx_collector/config-sample.json $DIR/../tools/hadoop_jmx_collector/config.json
command="python $DIR/../tools/hadoop_jmx_collector/hadoop_jmx_kafka.py"
job="* * * * * $command >> $DIR/../logs/hadoop_metric.log"
echo $job
cat <(fgrep -i -v "$command" <(crontab -l)) <(echo "$job") | crontab -

echo "$(crontab -l)"