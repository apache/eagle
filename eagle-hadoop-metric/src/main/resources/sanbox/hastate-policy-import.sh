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
echo "Importing policy: haStatePolicy "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
 -d '
 [
     {
       "prefix": "alertdef",
       "tags": {
         "site": "sandbox",
         "dataSource": "hadoopJmxMetricDataSource",
         "policyId": "haStatePolicy",
         "alertExecutorId": "hadoopJmxMetricAlertExecutor",
         "policyType": "siddhiCEPEngine"
       },
       "description": "jmx metric ",
       "policyDef": "{\"expression\":\"from every a = hadoopJmxMetricEventStream[metric==\\\"hadoop.namenode.fsnamesystem.hastate\\\"] -> b = hadoopJmxMetricEventStream[metric==a.metric and b.host == a.host and (convert(a.value, \\\"long\\\") != convert(value, \\\"long\\\"))] within 10 min select a.host, a.value as oldHaState, b.value as newHaState, b.timestamp as timestamp, b.metric as metric, b.component as component, b.site as site insert into tmp; \",\"type\":\"siddhiCEPEngine\"}",
       "enabled": true,
       "dedupeDef": "{\"alertDedupIntervalMin\":10,\"emailDedupIntervalMin\":10}",
       "notificationDef": "[{\"notificationType\":\"eagleStore\"},{\"sender\":\"eagle@apache.org\",\"recipients\":\"eagle@apache.org\",\"subject\":\"missing block found.\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]"
     }
 ]
 '

 ## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
