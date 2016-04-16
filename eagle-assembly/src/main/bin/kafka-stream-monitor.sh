#!/bin/bash

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

source $(dirname $0)/eagle-env.sh

if [ -e ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar ];then
	export jar_name=$(ls ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar)
else
	echo "ERROR: ${EAGLE_HOME}/lib/topology/eagle-topology-*-assembly.jar not found"
	exit 1
fi

export main_class=org.apache.eagle.datastream.storm.KafkaStreamMonitor

export alert_stream=$1
export alert_executor=$2
export config_file=$3

if [ "$#" -lt "2" ];then
	echo "ERROR: parameter required"
	echo "Usage: kafka-stream-monitor.sh [alert_stream alert_executor config_file] or [alert_stream config_file]"
	echo ""
	exit 1
fi
if [ "$#" == "2" ];then
	config_file=$2
	alert_executor="${alert_stream}Executor"
fi

cmd="java -cp $EAGLE_STORM_CLASSPATH:$jar_name $main_class -D eagle.stream.name=$alert_stream -D eagle.stream.executor=$alert_executor -D config.file=$config_file -D envContextConfig.jarFile=$jar_name"

echo "=========="
echo "Alert Stream: $alert_stream"
echo "Alert Executor: $alert_executor"
echo "Alert Config: $config_file"
echo "=========="
echo "Run Cmd: $cmd"
echo $cmd
$cmd