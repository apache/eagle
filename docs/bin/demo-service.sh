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

BASE_DIR="$(dirname $0)"
ROOT_DIR=$(cd "${BASE_DIR}/../"; pwd)
BASE_NAME="$(basename $0)"
SHELL_NAME="${BASE_NAME%.*}"
CONF_FILE="${BASE_DIR}/doc-env.conf"

if [ ! -f $CONF_FILE ]; then
	echo "file missing: ${CONF_FILE}"
fi

source ${CONF_FILE}

LOG_DIR="log"
TEMP_DIR="temp"
FULL_NAME="${PROGRAM}-${SHELL_NAME}-${PORT}"
LOG_FILE="${ROOT_DIR}/${LOG_DIR}/${FULL_NAME}.out"
PID_FILE="${ROOT_DIR}/${TEMP_DIR}/${FULL_NAME}-pid"

CURR_USER="$(whoami)"
echo -n "[sudo] password for ${CURR_USER}: "
read -s PWD
echo 

if [ ! -e ${ROOT_DIR}/${LOG_DIR} ]; then
	echo ${PWD} | sudo mkdir -p ${ROOT_DIR}/${LOG_DIR}
	echo ${PWD} | sudo chown -R ${USER}:${GROUP} ${ROOT_DIR}/${LOG_DIR}
	echo ${PWD} | sudo chmod -R ${FILE_MOD} ${ROOT_DIR}/${LOG_DIR}
fi

if [ ! -e ${ROOT_DIR}/${TEMP_DIR} ]; then
	echo ${PWD} | sudo mkdir -p ${ROOT_DIR}/${TEMP_DIR}
	echo ${PWD} | sudo chown -R ${USER}:${GROUP} ${ROOT_DIR}/${TEMP_DIR}
	echo ${PWD} | sudo chmod -R ${FILE_MOD} ${ROOT_DIR}/${TEMP_DIR}
fi

cd ${ROOT_DIR}

start() {
	echo "Starting ${FULL_NAME} ..."
	nohup ${COMMAND} 1> ${LOG_FILE} & echo $! > $PID_FILE
	if [ $? != 0 ];then
		echo "Error: failed starting"
		exit 1
	fi
	echo "Started successfully"
}

stop() {
    echo "Stopping ${FULL_NAME} ..."
	if [[ ! -f ${PID_FILE} ]];then
	    echo "No ${PROGRAM} running"
    	exit 1
    fi

    PID=`cat ${PID_FILE}`
	kill ${PID}
	if [ $? != 0 ];then
		echo "Error: failed stopping"
		rm -rf ${PID_FILE}
		exit 1
	fi

	rm ${PID_FILE}
	echo "Stopped successfully"
}

case $1 in
"start")
    start;
	;;
"stop")
    stop;
	;;
"restart")
	echo "Restarting ${FULL_NAME} ..."
    stop; sleep 1; start;
	echo "Restarting completed"
	;;
"status")
	echo "Checking ${FULL_NAME} status ..."
	if [[ -e ${PID_FILE} ]]; then
	    PID=`cat $PID_FILE`
	fi
	if [[ -z ${PID} ]];then
	    echo "Error: ${FULL_NAME} is not running (missing PID)"
	    exit 0
	elif ps -p ${PID} > /dev/null; then
	    echo "${FULL_NAME} is running with PID: ${PID}"
	    exit 0
    else
        echo "${FULL_NAME} is not running (tested PID: ${PID})"
        exit 0
    fi
	;;
*)
	print_help
	;;
esac

exit 0
