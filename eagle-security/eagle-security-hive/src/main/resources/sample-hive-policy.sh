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

#curl -X POST -H 'Content-Type:application/json' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"hiveQueryLogFromRunningJob","policyId":"accessSensitiveResource","alertExecutorId":"hiveAccessAlertByJobHistory","policyType":"siddhiCEPEngine"},"desc":"alert when some users access sensitive hive resource","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hiveAccessLogStream[user=='userxyz' and sensitivityType=='PHONE_NUMBER'] select user,timestamp,resource,sensitivityType insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":true}]'
#curl -X POST -H 'Content-Type:application/json' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"hiveQueryLog","policyId":"accessSensitiveResource","alertExecutorId":"hiveAccessAlertByRunningJob","policyType":"siddhiCEPEngine"},"desc":"alert when some users access sensitive hive resource","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hiveAccessLogStream[sensitivityType=='\'PHONE_NUMBER\''] select user,timestamp,resource,sensitivityType insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":true}]'
curl -X POST -H 'Content-Type:application/json' "http://localhost:9099/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"hiveQueryLog","policyId":"accessSensitiveResource","alertExecutorId":"hiveAccessAlertByRunningJob","policyType":"siddhiCEPEngine"},"desc":"alert when some users access sensitive hive resource","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hiveAccessLogStream[user=='\'userxyz\'' and sensitivityType=='\'PHONE_NUMBER\''] select user,timestamp,resource,sensitivityType insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":true}]'
