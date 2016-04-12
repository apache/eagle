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

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

source $base_dir/bin/eagle-env.sh

export EAGLE_CLASSPATH=$EAGLE_CLASSPATH:$(ls $EAGLE_HOME/lib/userprofile/eagle-security-userprofile-training-*-assembly.jar)
export EAGLE_CLASSPATH=$EAGLE_CLASSPATH:$(ls $EAGLE_HOME/lib/topology/eagle-topology-*.jar)

if [ -z "$EAGLE_JMX_OPTS" ]; then
  export EAGLE_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# Log directory to use
if [ "x$EAGLE_LOG_DIR" = "x" ]; then
    EAGLE_LOG_DIR="$base_dir/logs"
fi

# create logs directory
if [ ! -d "$EAGLE_LOG_DIR" ]; then
	mkdir -p "$EAGLE_LOG_DIR"
fi

# Log4j settings
if [ -z "$EAGLE_LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  EAGLE_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/conf/tools-log4j.properties"
else
  # create logs directory
  if [ ! -d "$EAGLE_LOG_DIR" ]; then
    mkdir -p "$EAGLE_LOG_DIR"
  fi
fi

# Generic jvm settings you want to add
if [ -z "$EAGLE_OPTS" ]; then
  EAGLE_OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$EAGLE_HEAP_OPTS" ]; then
  EAGLE_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$EAGLE_JVM_PERFORMANCE_OPTS" ]; then
  EAGLE_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$EAGLE_LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$EAGLE_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  EAGLE_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $EAGLE_HEAP_OPTS $EAGLE_JVM_PERFORMANCE_OPTS $EAGLE_GC_LOG_OPTS $EAGLE_JMX_OPTS $EAGLE_LOG4J_OPTS -cp $EAGLE_CLASSPATH $EAGLE_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $EAGLE_HEAP_OPTS $EAGLE_JVM_PERFORMANCE_OPTS $EAGLE_GC_LOG_OPTS $EAGLE_JMX_OPTS $EAGLE_LOG4J_OPTS -cp $EAGLE_CLASSPATH $EAGLE_OPTS "$@"
fi