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

export EAGLE_BASE_DIR=$(dirname $0)/../../../
export EAGLE_BUILD_DIR=${EAGLE_BASE_DIR}/eagle-assembly/target/eagle-*-bin/eagle-*/
cd $EAGLE_BUILD_DIR/

bin/kafka-producer.sh --broker-list localhost:6667 \
	--topic cassandra_querylog_local \
	--data '{"host": "/192.168.6.227","source": "/192.168.6.227","user": "jaspa","timestamp": 1455574202864, "category": "QUERY","type": "CQL_SELECT","ks": "dg_keyspace","cf": "customer_details","operation": "CQL_SELECT","masked_columns": "bank|ccno|email|ip|name|sal|ssn|tel|url","other_columns": "id|npi"}'