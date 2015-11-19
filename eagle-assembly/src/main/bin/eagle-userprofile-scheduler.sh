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

function usage() {
	echo "Usage: $0 [options] {start|stop|status}"
	echo ""
	echo "Commands"
	echo "  start                       Start scheduler process"
	echo "  stop                        Stop scheduler process"
	echo "  status                      Check scheduler process status"
	echo ""
	echo "Options:"
	echo "  --config <configFilePath>     Configuration file path"
	echo "  --log    <logFilePath>        Log file path"
	echo "  --daemon                      Run as daemon service"
	echo "	--site		site information"
}

source $(dirname $0)/eagle-env.sh

[ ! -e $EAGLE_HOME/logs ] && mkdir -p $EAGLE_HOME/logs
[ ! -e $EAGLE_HOME/temp ] && mkdir -p $EAGLE_HOME/temp

SCHEDULER_JAR=$(ls $EAGLE_HOME/lib/userprofile/eagle-security-userprofile-training-*-assembly.jar)
SCHEDULER_CLASS="org.apache.eagle.security.userprofile.daemon.Scheduler"
SCHEDULER_JVM_OPTS="-server"
SCHDULER_LOG_DIR=$(dirname $0)/logs/

SCHEDULER_CLASSPATH=$EAGLE_HOME/conf:$SCHEDULER_JAR
# Add eagle shared library jars
for file in $EAGLE_HOME/lib/share/*;do
	SCHEDULER_CLASSPATH=$SCHEDULER_CLASSPATH:$file
done

# Walk around comman-math3 conflict with spark
SCHEDULER_OPTS="-D eagle.userprofile.driver-classpath=$(dirname $0)/../lib/share/asm-3.1.jar:$(dirname $0)/../lib/share/commons-math3-3.5.jar"

# Specify user profile spark job assembly jar
SCHEDULER_OPTS="$SCHEDULER_OPTS -D eagle.userprofile.jar=$SCHEDULER_JAR"

SCHEDULER_CMD="java $SCHEDULER_JVM_OPTS -cp $SCHEDULER_CLASSPATH:$SCHEDULER_JAR $SCHEDULER_CLASS $SCHEDULER_OPTS"

### parameters are in pair
if [ $# = 0 ] ; then
	usage
	exit 1
fi

while [  $# -gt 0  ]; do
case $1 in
  "--site")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 site=$2
     shift 2
     ;;
  "--config")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 config=$2
     shift 2
     ;;
  "--log")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
	 log=$2
     shift 2
     ;;
  "--daemon")
     daemon="true"
     shift 1
     ;;
  "start")
    command="start"
     shift 1
    ;;
  "stop")
    command="stop"
	shift 1
    ;;
  "status")
    command="status"
	shift 1
    ;;
  *)
     echo "Internal Error: option processing error: $1" 1>&2
     usage
     exit 1
     ;;
  esac
done

# Validate Arguments
# ==================

# --site
if [ -z "$site" ];then
	echo "Error: --site required"
	usage
	exit 1
fi

pid=$(dirname $0)/../temp/$site-userprofile-scheduler.pid

# --config
if [ -z "$config" ];then
	config=$(dirname $0)/../conf/$site-userprofile-scheduler.conf
fi

# --log
if [ -z "$log" ];then
	log=$(dirname $0)/../logs/$site-userprofile-scheduler.out
fi

# Define Functions
# ================
function start(){
	if [ -e $pid ];then
		pidv=`cat $pid`
		ps -p $pidv 1>/dev/null 2>&1
		if [ "$?" != "0" ];then
			echo "Process [$pidv] is found, but dead, continue to start ..."
		else
			echo "Process [$pidv] is still runing, please top it firstly, exiting ..."
			exit 1
		fi
	fi

	if [ ! -e $config ];then
		echo "Error: --config $config not exist"
		usage
		exit 1
	fi

	cmd="$SCHEDULER_CMD -D eagle.site=$site -D config.file=$config"

	if [ "$daemon" == "true" ];then
		echo "Executing: $cmd as daemon"
		echo $cmd >> $log
		nohup $cmd 1>>$log 2>&1 &
		pidv=$!
		echo $pidv > $pid
		echo "Logging to: $log, pid: $pidv"
	else
		echo "Executing: $cmd"
		$cmd
	fi
}

function stop(){
	if [ -e $pid ];then
		pidv=`cat $pid`
		ps -p $pidv 1>/dev/null 2>&1
		if [ "$?" != "0" ];then
			rm $pid
			echo "Process [$pidv] is not running, but PID file exisits: $pid, removed"
			exit 1
		else
			echo "Killing process [$pidv]"
			kill $pidv
			if [ $? == 0 ];then
				rm $pid
				echo "Killed successfully"
				exit 0
			else
				echo "Failed to kill process [$pid]"
				exit 1
			fi
		fi
	else
		echo "Process is not running"
		exit 1
	fi
}

function status(){
	if [ -e $pid ];then
		pidv=`cat $pid`
		ps -p $pidv 1>/dev/null 2>&1
		if [ "$?" != "0" ];then
			echo "Process [$pidv] is dead"
			exit 1
		else
			echo "Process [$pidv] is running"
			exit 0
		fi
	else
		echo "$pid not found, assume process should have been stopped"
		exit 1
	fi
}

case $command in
	"start")
		start
		;;
	"stop")
		stop
		;;
	"status")
		status
		;;
	*)  usage
		exit 1
		;;
esac