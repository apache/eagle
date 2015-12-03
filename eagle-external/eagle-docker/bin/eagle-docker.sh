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

# NOTICE: This script is developed and maintained by Apache Eagle community under Apache Softwarw Foundation but not from official Docker product or community.

EAGLE_DOCKER_VERSION=latest
EAGLE_DOCKER_NAME=apacheeagle/sandbox
EAGLE_DOCKER_PREFIX=sandbox

export NODE_NUM=1

cd `dirname $0`/../
source bin/eagle-lib.sh	

function check_env(){
	which docker 1>/dev/null 2>&1

	if [ $? != 0 ];then
		echo "[ERROR] docker commnad is not found, please make sure install and configure Docker Engine properly, docker docs: http://docs.docker.com" 1>&2
		exit 1
	else
		log=`docker images 2>&1`
		if [ $? != 0 ];then
			echo "[ERROR] $log"
			exit 1
		fi
	fi
}

function usage() {
	echo "Usage: ./eagle-docker [options] [command]"

	echo ""
	echo "Apache Eagle Docker Image : ${EAGLE_DOCKER_NAME}:${EAGLE_DOCKER_VERSION}"
	echo ""
	echo "Commands:"
	echo "  build           Build eagle docker image"
	echo "  deploy          Deploy eagle docker image"
	echo "  start           Start eagle docker instance"
	echo "  stop            Stop eagle docker instance"
	echo "  status          List eagle docker image and instance status"
	echo "  clean           Clean docker image and instance"
	echo "  shell           Execute docker instance bash, default: eagle-sandbox"
	echo "  boot            Simply bootstrap eagle docker by building then deploying"
	echo ""
	echo "Options:"
	echo "  --node [num]    Docker instances node number, default is 1"
	echo "  --help          Display eagle docker image usage information"
	echo ""
	exit 0
}

function build(){
	check_env
	# ==========================================
	# Check Eagle Docker Image
	# ==========================================
	echo "[1/3] Validate Environment"
	# echo "Checking eagle docker image ..."
	docker images | grep $EAGLE_DOCKER_NAME 1>/dev/null 2>&1
	if [ $? == 0 ];then
		echo "Eagle docker image already exists:"
		echo ""
		docker images | grep $EAGLE_DOCKER_NAME
		echo ""
		echo "Rebuild it now [Y/N]? "
		read rebuild
		if [ $rebuild == "Y" ] || [ $rebuild == "y" ];then
			echo  "Clean eagle image"
			clean
		else
			echo "Quit now"
			exit 1
		fi
 	else
		echo "No existing eagle docker images, environment is ready"
	fi	

	# ==========================================
	# Build Eagle Binary Package 
	# ==========================================
	echo "[2/3] Build Eagle Binary Package"
	# echo "Checking eagle binary package"
	cd ../../
	ls eagle-assembly/target/eagle-*-bin.tar.gz 1>/dev/null 2>&1
	if [ $? == 0 ];then
		eagle_bin_pkg=`ls eagle-assembly/target/eagle-*-bin.tar.gz`
		echo "Found eagle binary package at $eagle_bin_pkg"	
	else
		echo "Eagle binary package is not found"
		echo "Build eagle binary package now"
		# ==========================================
		# Build Eagle Binary Package with Maven
		# ==========================================
		echo ""
		echo "Execute: mvn package -DskipTests "
		mvn package -DskipTests
		if [ $? == 0 ];then
			echo "Built successfully existing with 0"	
			ls eagle-assembly/target/eagle-*-bin.tar.gz 1>/dev/null 2>&1
			if [ $? == 0 ];then
				eagle_bin_pkg=`ls eagle-assembly/target/eagle-*-bin.tar.gz`
				echo ""
				echo "[SUCCESS] Successfully build eagle binary package at $eagle_bin_pkg"	
			else
				echo ""
				echo "[FAILED] Built eagle binary package exiting with 0, but package is not found"
				exit 1
			fi
		else
			echo ""
			echo "[FAILED] Failed to build eagle binary package, exit 1"
			exit 1
		fi
	fi

  	# =====================================
	# Build Eagle Docker Image
  	# =====================================
	echo "[3/3] Build Eagle Docker Image: $EAGLE_DOCKER_NAME"
	echo "Extracting $eagle_bin_pkg" 
	if [ -e eagle-external/eagle-docker/target ];then
		rm -rf eagle-external/eagle-docker/target
	fi
	mkdir -p eagle-external/eagle-docker/target/eagle-current

	out=`tar -xzf $eagle_bin_pkg -C eagle-external/eagle-docker/target/`
	if [ $? != 0 ];then
		echo "[ERROR] Failed to execute 'tar -xzf $eagle_bin_pkg -C eagle-external/eagle-docker/target/': $out" 1>&2 
		exit 1
	fi
	mv eagle-external/eagle-docker/target/eagle-*/* eagle-external/eagle-docker/target/eagle-current

	echo "Execute: docker build -t $EAGLE_DOCKER_NAME ."
        cd eagle-external/eagle-docker
	docker build -t $EAGLE_DOCKER_NAME .
	
	if [ $? == 0 ];then
		echo ""
		echo "[SUCCESS] Successfully built eagle docker image : $EAGLE_DOCKER_NAME"
	else
		echo ""
		echo "[FAILED] Failed to build eagle docker image : $EAGLE_DOCKER_NAME"
	fi
}


function start(){
	check_env
	docker ps -a | grep $EAGLE_DOCKER_NAME 1>/dev/null 
	if [ $? != 0 ];then
		echo "No eagle docker instance found"
		exit 1
	fi
	docker ps -a | grep $EAGLE_DOCKER_NAME | grep Exited  1>/dev/null 
	if [ $? != 0 ];then
		echo "No exited eagle docker instance found, all should be running"
		exit 1
	fi
	docker ps -a | grep $EAGLE_DOCKER_NAME | grep Exited | awk '{ print $1 }' | while read id; do echo "Starting $id";docker start $id 1>/dev/null;done

	echo ""
	echo "[Eagle Docker Instances (Up)]"	
	echo ""
	docker ps | grep $EAGLE_DOCKER_NAME
}

function stop(){
	check_env
	docker ps | grep $EAGLE_DOCKER_NAME  1>/dev/null
	if [ $? != 0 ];then
		echo "No eagle docker instance is running"
		exit 1
	fi
	docker ps | grep $EAGLE_DOCKER_NAME  | awk '{ print $1 }' | while read id; do echo "Stopping $id";docker kill $id;done

	echo ""
	echo "[Eagle Docker Instances (Exited)]"	
	echo ""
	docker ps -a| grep $EAGLE_DOCKER_NAME
}

function status(){
	check_env
	echo "[Eagle Docker Images]"
	echo ""
	docker images | grep $EAGLE_DOCKER_NAME
	[[ $? == 0 ]] || echo "no images found for eagle"
	echo ""

	echo "[Eagle Docker Instances]"	
	echo ""
	docker ps -a | grep $EAGLE_DOCKER_NAME 
	[[ $? == 0 ]] || echo "no instances found eagle"
	echo ""
}


function clean(){
	check_env
	echo "[1/4] Stop eagle instances"
	# Call in multi-process to avoid exiting by stopping
	echo `stop`
	
	echo "[2/4] Remove eagle instances"
	docker ps -a | grep $EAGLE_DOCKER_NAME |awk '{print $1}'| while read id; do echo "Removing instance $id";docker rm $id;done

	echo "[3/4] Remove eagle images"
	docker images | grep $EAGLE_DOCKER_NAME | awk '{print $3}' | while read id; do echo "Removing image $id"; docker rmi $id;done

	echo "[4/4] Clean temp files"
        [[ -e target/ ]] && rm -rf target/
	
	echo "Cleaning Done."
}

function deploy(){
	check_env
	if [ "$NODE_NUM" == "" ];then
		export NODE_NUM=1
	fi 
	echo "Deploying $NODE_NUM nodes"
	eagle-deploy-cluster $NODE_NUM
}

function boot(){
	check_env
	build
	deploy
	status
}

function exec_bash(){
	docker exec -it $EAGLE_DOCKER_PREFIX bash
}

while [[ -n $1 ]]; do
	case $1 in
	"--node")
		if [ $# -lt 2 ]; then
        	usage
        	exit 1
     		fi
	 	NODE_NUM=$2
     		shift
     		;;
	"deploy")
        	deploy 
		exit
		;;
	"build")
        	build	
		exit
		;;
	"boot")
        	boot	
		exit
		;;
	"status")
        	status 
		exit
		;;
	"start")
 		start	
		exit
		;;
	"stop")
 		stop	
		exit
		;;
	"clean")	
		clean	
		exit
		;;
	"shell")	
		exec_bash
		exit
		;;
	*)
		usage
		exit 1
	;;
	esac
	shift
done
