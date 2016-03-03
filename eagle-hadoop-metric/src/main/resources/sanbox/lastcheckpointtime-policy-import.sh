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
source $(dirname $0)/hadoop-metric-init.sh


##### add policies ##########
echo ""
echo "Importing policy: lastCheckPointTimePolicy "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
     {
       "prefix": "alertdef",
       "tags": {
         "site": "sandbox",
         "dataSource": "hadoopJmxMetricDataSource",
         "policyId": "lastCheckPointTimePolicy",
         "alertExecutorId": "hadoopJmxMetricAlertExecutor",
         "policyType": "siddhiCEPEngine"
       },
       "description": "jmx metric ",
       "policyDef": "{\"expression\":\"from hadoopJmxMetricEventStream[metric == \\\"hadoop.namenode.dfs.lastcheckpointtime\\\" and (convert(value, \\\"long\\\") + 18000000) < timestamp]#window.externalTime(timestamp ,10 min) select metric, host, value, timestamp, component, site insert into tmp; \",\"type\":\"siddhiCEPEngine\"}",
       "enabled": true,
       "dedupeDef": "{\"alertDedupIntervalMin\":10,\"emailDedupIntervalMin\":10}",
       "notificationDef": "[{\"notificationType\":\"eagleStore\"},{\"sender\":\"eagle@apache.org\",\"recipients\":\"eagle@apache.org\",\"subject\":\"last check point time lag found.\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]"
     }
 ]
 '

 ## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
