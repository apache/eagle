#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with`
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

source $(dirname $0)/eagle-env.sh
source $(dirname $0)/gclog-init-sanbox.sh
##### add policies ##########
echo " Creating policy NNGCPauseTimeLong for GCLog Monitoring "

#### AlertDefinitionService: alert definition for NNGCLog Pause Time Long
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
 {"tags":{"site":"sandbox","dataSource":"NNGCLog","policyId":"NNGCPauseTimeLong","alertExecutorId":"NNGCAlert","policyType":"siddhiCEPEngine"},"desc":"alert when namenode pause time long","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from NNGCLogStream#window.externalTime(timestamp,10 min) select sum(pausedGCTimeSec) as sumPausedSec having sumPausedSec >= 30 insert into outputStream;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":true}
 ]
 '

 echo " Created Policy  NNGCPauseTimeLong Successfully  ...."

