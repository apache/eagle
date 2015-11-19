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
	echo "Usage: $0 --jar <jarName> --main <mainClass> --site <sitename> --input <inputschema> --output <outputschema>"
	echo "--site <siteName>     Must be given"
	echo "--jar <jarName>       Default is $EAGLE_HOME/lib/userprofile/eagle-security-userprofile-training-0.1.0-assembly.jar"
    echo "--main <mainClass>    Default is org.apache.eagle.security.userprofile.UserProfileTrainingCLI"
    echo "--input               Default is /tmp/userprofile/hdfs-audit.log"
    echo "--output              Default is eagle://localhost:9099. When modelSink is hdfs, the value is hdfs:///tmp/userprofile/output"
	echo "Example: $0 --jar <jarName> --main <mainClass> --site <sitename> --input <input> --output <output>"
}

function env_check(){
    which spark-submit >/dev/null 2>&1
    if [ $? != 0 ];then
        echo "Error: spark is not installed"
        exit 1
    fi
}

source $(dirname $0)/eagle-env.sh

#### parameters are in pair
if [ $# = 0 ] || [ `expr $# % 2` != 0 ] ; then
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
  "--jar")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
     jar=$2
     shift 2
     ;;
  "--main")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
     main=$2
     shift 2
     ;;
  "--input")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
     input=$2
     shift 2
     ;;
  "--output")
     if [ $# -lt 2 ]; then
        usage
        exit 1
     fi
     output=$2
     shift 2
     ;;
  *)
     echo "Internal Error: option processing error: $1" 1>&2
     exit 1
     ;;
  esac
done

if [ -z "$site" ];then
	echo "Error: --site is required" 1>&2
	exit 1
fi

if [ -z "$main" ] ; then
  main="org.apache.eagle.security.userprofile.UserProfileTrainingCLI"
fi

if [ -z "$jar" ] ; then
  jar="$EAGLE_HOME/lib/userprofile/eagle-security-userprofile-training-0.1.0-assembly.jar"
fi

if [ -z "input" ] ; then
  input="hdfs:///tmp/userprofile/hdfs-audit.log"
fi

if [ -z "output" ] ; then
  output="eagle://localhost:9099"
fi

outputScheme=`echo $output | sed -E 's/^(.*):\/\/.*/\1/'`

case $outputScheme in
 "hdfs")
   env_check
   echo "Starting eagle user profile training ..."
   hdfsOutput=`echo $output | sed -E 's/hdfs:\/\/(.*)/\1/'`
   spark-submit --class $main \
                 --master "local[10]" \
                 --driver-class-path $EAGLE_HOME/lib/share/asm-3.1.jar:$EAGLE_HOME/lib/share/commons-math3-3.5.jar \
                 $jar \
                 --site $site \
                 --input $input \
                 --output $hdfsOutput
    if [ $? = 0 ]; then
		echo "Starting is completed"
		exit 0
	else
		echo "Failure"
		exit 1
	fi
	;;
  "eagle")
    env_check
    echo "Starting eagle user profile training ..."
    eagleServiceHost=`echo $output | sed -E 's/eagle:\/\/(.*):(.*)/\1/'`
    eagleServicePort=`echo $output | sed -E 's/eagle:\/\/(.*):(.*)/\2/'`
    spark-submit --class $main \
                 --master "local[10]" \
                 --driver-class-path $EAGLE_HOME/lib/share/asm-3.1.jar:$EAGLE_HOME/lib/share/commons-math3-3.5.jar \
                 $jar \
                 --site $site \
                 --input $input \
                 --service-host $eagleServiceHost \
                 --service-port $eagleServicePort

    if [ $? = 0 ]; then
        echo "Starting is completed"
        exit 0
    else
        echo "Failure"
        exit 1
    fi
    ;;
  *)
	usage
	exit 1
esac

exit 0