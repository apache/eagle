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

TOPOLOGY_NAME_SET=0

function print_help() {
	echo "Usage: $0 options {start | stop | status}"
	echo "Options:                       Description:"
	echo "  --jar      <jar path>          Default is $EAGLE_HOME/lib/topology/eagle-topology-0.1.0-assembly.jar"
	echo "  --main     <main class>        Default is org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain"
	echo "  --topology <topology name>     Default is sandbox-hdfsAuditLog-topology"
	echo "  --config   <file path>         Default is $EAGLE_HOME/conf/sandbox-hdfsAuditLog-application.conf"
	echo "  --storm-ui <storm ui url>      Execute through storm UI API, default: http://localhost:8744"

	echo "Command Examples:"
	echo "  $0 [--jar <jarPath>] --main <mainClass> [--topology <topologyName>] --config <filePath> start"
	echo "  $0 --topology <topologyName> stop"
	echo "  $0 --topology <topologyName> status"
}

function env_check(){
    which storm >/dev/null 2>&1
    if [ $? != 0 ];then
        echo "Error: storm is not installed"
        exit 1
    fi
}

#### parameters are in pair plus command
#if [ `expr $# % 2` != 1 ]
#then
#    print_help
#    exit 1
#fi

cmd=""
while [  $# -gt 0  ]; do
case $1 in
    "start")
        cmd=$1
        shift
        ;;
    "stop")
        cmd=$1
        shift
        ;;
    "status")
        cmd=$1
        shift
        ;;
    --jar)
         if [ $# -lt 3 ]; then
             print_help
             exit 1
         fi
         jarName=$2
         shift 2
         ;;
    --main)
        if [ $# -lt 3 ]; then
            print_help
            exit 1
        fi
        mainClass=$2
        shift 2
        ;;
    --topology)
        if [ $# -lt 3 ]; then
            print_help
            exit 1
        fi
        TOPOLOGY_NAME_SET=1
        topologyName=$2
        shift 2
        ;;
    --config)
        if [ $# -lt 3 ]; then
            print_help
            exit 1
        fi
        configFile=$2
        shift 2
        ;;
	--storm-ui)
		# TODO: configure through arguments
		storm_ui="http://localhost:8744"
		shift 1
		;;
    *)
        echo "Internal Error: option processing error: $1" 1>&2
        exit 1
        ;;
    esac
done


if [ -z "$jarName" ]; then
     jarName="${EAGLE_HOME}/lib/topology/eagle-topology-0.1.0-assembly.jar"
fi

if [ -z "$mainClass" ]; then
    mainClass="org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain"
fi

if [ -z "$topologyName" ]; then
    topologyName=sandbox-hdfsAuditLog-topology
fi

if [ -z "$configFile" ]; then
    configFile="$EAGLE_HOME/conf/sandbox-hdfsAuditLog-application.conf"
fi

case $cmd in
"start")
    env_check
    echo "Starting eagle topology ..."
    echo "jarName="$jarName "mainClass="$mainClass "configFile="$configFile
    if [ $TOPOLOGY_NAME_SET = 1 ]; then
	    storm jar -c nimbus.host=$EAGLE_NIMBUS_HOST ${jarName} $mainClass -D config.file=$configFile -D envContextConfig.topologyName=$topologyName
	else
	    storm jar -c nimbus.host=$EAGLE_NIMBUS_HOST ${jarName} $mainClass -D config.file=$configFile
	fi
	if [ $? = 0 ]; then
		echo "Starting is completed"
		exit 0
	else
		echo "Failure"
		exit 1
	fi
	;;
"stop")
    env_check
    echo "Stopping eagle topology ..."
    storm kill -c nimbus.host=$EAGLE_NIMBUS_HOST $topologyName
    if [ $? = 0 ]; then
    	echo "Stopping is completed"
	    exit 0
    else
    	echo "Failure"
	exit 1
    fi
	;;
"status")
	if [ -z "$storm_ui" ];then
	    env_check
	    echo "Checking topology $topologyName status ..."
	    output=`storm list  -c nimbus.host=$EAGLE_NIMBUS_HOST | grep $topologyName`
	    if [ $? != 0 ];then
	        echo "Topolog is not alive: $topologyName is not found"
	        exit 1
	    fi

	    echo $output | grep ACTIVE > /dev/null 2>&1

	    if [ $? = 0 ];then
	        echo "Topology is alive: $output"
	        exit 0
	    else
	        echo "Topology is not alive: $output"
	        exit 1
	    fi
    else
	    echo "Checking topology $topologyName status through storm UI API on $storm_ui"
	    curl -XGET $storm_ui/api/v1/topology/summary | grep $topologyName | grep ACTIVE >/dev/null 2>&1
	    if [ $? == 0 ];then
	        echo "$topologyName is running"
	        exit 0
	    else
	        echo "$topologyName is dead"
	        exit 1
	    fi
    fi
    ;;
*)
	print_help
	exit 1
esac

exit 0