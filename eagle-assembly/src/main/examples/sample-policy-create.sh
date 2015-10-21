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

source $(dirname $0)/../bin/eagle-env.sh

su hdfs -c "hdfs dfs -touchz /tmp/private"
#su hdfs -c "hdfs dfs -touchz /tmp/sensitive"

#### create hdfs policy sample in sandbox
echo "create hdfs policy sample in sandbox... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d \
'[{"tags":{"site":"sandbox","dataSource":"hdfsAuditLog","alertExecutorId":"hdfsAuditLogAlertExecutor","policyId":"viewPrivate","policyType":"siddhiCEPEngine"},"desc":"view private file","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hdfsAuditLogEventStream[(cmd=='\'open\'') and (src=='\'/tmp/private\'')] select * insert into outputStream\"}","dedupeDef": "{\"alertDedupIntervalMin\":0,\"emailDedupIntervalMin\":1440}","notificationDef": "[{\"subject\":\"just for test\",\"sender\":\"nobody@test.com\",\"recipients\":\"nobody@test.com\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]","remediationDef":"","enabled":true}]'

#### create hive policy sample in sandbox
echo "create hive policy sample in sandbox... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d \
'[{"tags":{"site":"sandbox","dataSource":"hiveQueryLog","alertExecutorId":"hiveAccessAlertByRunningJob","policyId":"queryPhoneNumber","policyType":"siddhiCEPEngine"},"desc":"query sensitive hive data","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hiveAccessLogStream[(sensitivityType=='\'PHONE_NUMBER\'')] select * insert into outputStream;\"}","dedupeDef": "{\"alertDedupIntervalMin\":0,\"emailDedupIntervalMin\":1440}","notificationDef": "[{\"subject\":\"just for test\",\"sender\":\"nobody@test.com\",\"recipients\":\"nobody@test.com\",\"flavor\":\"email\",\"id\":\"email_1\",\"tplFileName\":\"\"}]","remediationDef":"","enabled":"true"}]'
