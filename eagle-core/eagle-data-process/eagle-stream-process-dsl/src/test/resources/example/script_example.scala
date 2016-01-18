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
exec scala -classpath "/Users/hchen9/Workspace/incubator-eagle/eagle-topology-assembly/target/eagle-topology-0.3.0-assembly.jar" "$0" "$@"
!#
import org.apache.eagle.stream.dsl.StreamBuilder._

init[storm](args)

// define("metric") as("name" -> 'string, "value" -> 'double, "timestamp" -> 'long) from kafka parallism 10
//define("metric") as("name" -> 'string, "value" -> 'double, "timestamp" -> 'long) from Range(1,10000) parallism 1

"number" := stream from Range(1,10000) parallism 1

// filter ("metric") groupBy 0 by {line:Map[String,AnyRef] => line}

$"number" groupBy 0  stdout

submit