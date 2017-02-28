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

#: ${EAGLE_HOST:=$AMBARISERVER_PORT_9099_TCP_ADDR}
: ${EAGLE_HOST:=$EAGLE_SERVER_HOST}
: ${SLEEP:=2}
: ${DEBUG:=1}

: ${EAGLE_HOST:? eagle server address is mandatory, fallback is a linked containers exposed 9090}

debug() {
  [ $DEBUG -gt 0 ] && echo [DEBUG] "$@" 1>&2
}

get-server-state() {
  curl -s -o /dev/null -w "%{http_code}" $EAGLE_HOST:9090
}

SERF_RPC_ADDR=${EAGLE_SERVER_HOST}:7373
serf event --rpc-addr=$SERF_RPC_ADDR start-services
sleep 30
serf event --rpc-addr=$SERF_RPC_ADDR eagle

debug waiting for eagle to start on: $EAGLE_HOST
while ! get-server-state | grep 200 &>/dev/null ; do
  [ $DEBUG -gt 0 ] && echo -n .
  sleep $SLEEP
done
[ $DEBUG -gt 0 ] && echo
debug eagle web started: $EAGLE_HOST:9090
