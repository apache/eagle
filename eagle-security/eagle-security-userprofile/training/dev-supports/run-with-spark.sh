#!/usr/bin/env bash

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

which spark-submit 1>/dev/null 2>&1

if [ $? != 0 ];then
	echo "Error: spark-submit not found, please add spark-summit into environment PATH, exit code: 1"
	exit 1
fi

cd $(dirname $0)/../

cmd="mvn clean compile package -DskipTests"
echo -e "Execute: $cmd\n"
$cmd

if [ $? != 0 ];then
	echo "-----"
	echo "Failed to compile and package project"
	exit 1
else
	echo "Success to compile and package project"
fi

rm -rf /tmp/eagle-userprofile-output 1>/dev/null 2>&1
cmd="spark-submit --class eagle.security.userprofile.UserProfileTrainingCLI `ls target/eagle-security-userprofile-training-*-assembly.jar` --master local[10] --site sandbox --input `pwd`/src/main/resources/hdfs-audit.log --output /tmp/eagle-userprofile-output"
echo -e "Execute: $cmd\n"
$cmd

exit_code=$?
if [ $exit_code != 0 ];then
	echo "-----"
	echo "Failed to execute job on spark, exit code: $exit_code"
	exit 1
else
	echo "-----"
	echo "Success to execute job on spark, output: /tmp/eagle-userprofile-output, exit code: $exit_code"
fi