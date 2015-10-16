#!/usr/bin/env bash

#
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
#


which spark-submit 1>/dev/null 2>&1

if [ $? != 0 ];then
	echo "Error: spark-submit not found, please add spark-summit into environment PATH, exit code: 1"
	exit 1
fi

cd $(dirname $0)/../

export JAR_FILE=$(ls `pwd`/target/eagle-security-userprofile-training-*-assembly.jar)
export DRIVER_CLASSPATH=$(ls `pwd`/target/lib/asm-3.1.jar):$(ls `pwd`/target/lib/commons-math3-3.5.jar)

#if [ ! -e $JAR_FILE ]; then
#	echo "$JAR_FILE not exists, build package ..."
#	mvn clean package -DskipTests
#	JAR_FILE=$(ls `pwd`/target/eagle-security-userprofile-training-*-assembly.jar)
#else
#	echo "$JAR_FILE already exists"
#
#fi

# Force clean and re-package
mvn clean package -DskipTests

export AUDIT_LOG_PATH=$(ls `pwd`/src/main/resources/hdfs-audit.log)

mvn exec:java -DskipTests -Dexec.mainClass="eagle.security.userprofile.daemon.Scheduler"  \
	-Dexec.args="-D eagle.userprofile.driver-classpath=$DRIVER_CLASSPATH -D eagle.userprofile.jar=$JAR_FILE -D eagle.userprofile.training-audit-path=$AUDIT_LOG_PATH -D eagle.userprofile.detection-audit-path=$AUDIT_LOG_PATH $@"