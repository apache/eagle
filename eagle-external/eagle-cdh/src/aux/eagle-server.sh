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

function print_help() {
	echo "Usage: $0 {start | stop | restart | status}"
	exit 1
}

if [ $# != 1 ]
then
	print_help
fi

DIR=$(dirname $0)


source ${DIR}/eagle-env.sh

JVM_OPTS="-server -Xms1024m -Xmx1024m -XX:MaxPermSize=1024m"

GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:${DIR}/../log/eagle-server-gc.log"

if [ ! -z "$EAGLE_SERVER_JMX_PORT" ]; then
    JMX_HOST=`hostname -f`
    JVM_OPTS="-Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port=$EAGLE_SERVER_JMX_PORT \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Djava.rmi.server.hostname=$JMX_HOST \
    -Dcom.sun.management.jmxremote.rmi.port=$EAGLE_SERVER_JMX_PORT \
    $JVM_OPTS"
fi

JVM_OPTS="-Dconfig.resource=eagle.properties $JVM_OPTS $GC_OPTS"

PIDFILE="${DIR}/../run/eagle-server.pid"

CONFIGURATION_YML="${DIR}/../conf/server.yml"

DEBUG_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

PROGRAM="java -cp $EAGLE_CLASSPATH $JVM_OPTS org.apache.eagle.server.ServerMain server ${CONFIGURATION_YML}"

cd $DIR/../

if [ ! -e ${DIR}/../run ]; then
    mkdir ${DIR}/../run
fi

if [ ! -e ${DIR}/../log ]; then
    mkdir ${DIR}/../log
fi

start() {
    echo "Starting eagle service ..."
    echo ${PROGRAM}
    exec ${PROGRAM} >> /var/log/eagle/eagle-server.out
    if [ $? != 0 ];then
	echo "Error: failed starting"
	exit 1
    fi
}

stop() {
    echo "Stopping eagle service ..."
	if [[ ! -f $PIDFILE ]];then
	    echo "Eagle service is not running"
    	exit 1
    fi

    PID=`cat $PIDFILE`
	kill $PID
	if [ $? != 0 ];then
		echo "Error: failed stopping"
		rm -rf ${PIDFILE}
		exit 1
	fi

	rm ${PIDFILE}
	echo "Stopping is completed"
}

case $1 in
"start")
    start;
	;;
"stop")
    stop;
	;;
"restart")
	echo "Restarting eagle service ..."
    stop; sleep 1; start;
	echo "Restarting is completed "
	;;
"status")
	echo "Checking eagle service status ..."
	if [[ -e ${PIDFILE} ]]; then
	    PID=`cat $PIDFILE`
	fi
	if [[ -z ${PID} ]];then
	    echo "Error: Eagle service is not running (missing PID)"
	    exit 0
	elif ps -p ${PID} > /dev/null; then
	    echo "Eagle service is running with PID $PID"
	    exit 0
    else
        echo "Eagle service is not running (tested PID: ${PID})"
        exit 0
    fi
	;;
*)
	print_help
	;;
esac

if [ $? != 0 ]; then
	echo "Error: start failure"
	exit 1
fi
exit 0
