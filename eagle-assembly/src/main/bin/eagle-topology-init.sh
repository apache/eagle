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
eagle_bin=$EAGLE_HOME/bin


#####################################################################
#            Import stream metadata for HDFS
#####################################################################

## AlertDataSource: data sources bound to sites
echo "Importing AlertDataSourceService for HDFS... "

curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"hdfsAuditLog"}, "enabled": "true", "config" : "{\"hdfsEndpoint\":\"hdfs://sandbox.hortonworks.com:8020\"}", "desc":"HDFS"}]'


## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream"},"desc":"alert event stream from hdfs audit log"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hdfsAuditLog","alertExecutorId":"hdfsAuditLogAlertExecutor","streamName":"hdfsAuditLogEventStream"},"desc":"alert executor for hdfs audit log event stream"}]'

## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HDFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"src"},"attrDescription":"source directory or file, such as /tmp","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"dst"},"attrDescription":"destination directory, such as /tmp","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"host"},"attrDescription":"hostname, such as localhost","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"timestamp"},"attrDescription":"milliseconds of the datetime","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"allowed"},"attrDescription":"true, false or none","attrType":"bool","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"user"},"attrDescription":"process user","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"cmd"},"attrDescription":"file/directory operation, such as getfileinfo, open, listStatus and so on","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSCommandResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"sensitivityType"},"attrDescription":"mark such as AUDITLOG, SECURITYLOG","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSSensitivityTypeResolver"},{"prefix":"alertStreamSchema","tags":{"dataSource":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"securityZone"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":""}]'

#####################################################################
#            Import stream metadata for HIVE
#####################################################################

## AlertDataSource: data sources bound to sites
echo ""
echo "Importing AlertDataSourceService for HIVE... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"hiveQueryLog"},"enabled": "true", "config" : "{\"accessType\":\"metastoredb_jdbc\",\"password\":\"hive\",\"user\":\"hive\",\"jdbcDriverClassName\":\"com.mysql.jdbc.Driver\",\"jdbcUrl\":\"jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true\"}", "desc":"HIVE"}]'

## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HIVE... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream"},"desc":"alert event stream from hive query"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HIVE... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"dataSource":"hiveQueryLog","alertExecutorId":"hiveAccessAlertByRunningJob","streamName":"hiveAccessLogStream"},"desc":"alert executor for hive query log event stream"}]'

## AlertStreamSchemaServiceService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HIVE... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"process user","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"user"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"hive sql command, such as SELECT, INSERT and DELETE","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveCommandResolver","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"command"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrDescription":"milliseconds of the datetime","attrValueResolver":"","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"timestamp"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"/database/table/column or /database/table/*","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveMetadataResolver","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"resource"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"mark such as PHONE_NUMBER","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveSensitivityTypeResolver","tags":{"dataSource":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"sensitivityType"}}]'

#####################################################################
#            Import stream metadata for UserProfile
#####################################################################
echo ""
echo "Importing AlertDataSourceService for USERPROFILE"
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDataSourceService" -d '[{"prefix":"alertDataSource","tags":{"site" : "sandbox", "dataSource":"userProfile"}, "enabled": "true", "config" : "{\"features\":\"getfileinfo,open,listStatus,setTimes,setPermission,rename,mkdirs,create,setReplication,contentSummary,delete,setOwner,fsck\"}", "desc":"USERPROFILE"}]'

echo ""
echo "Importing AlertDefinitionService for USERPROFILE"
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
     -d '[ { "prefix": "alertdef", "tags": { "site": "sandbox", "dataSource": "userProfile", "alertExecutorId": "userProfileAnomalyDetectionExecutor", "policyId": "userProfile", "policyType": "MachineLearning" }, "desc": "user profile anomaly detection", "policyDef": "{\"type\":\"MachineLearning\",\"alertContext\":{\"site\":\"sandbox\",\"dataSource\":\"userProfile\",\"component\":\"testComponent\",\"description\":\"ML based user profile anomaly detection\",\"severity\":\"WARNING\",\"notificationByEmail\":\"true\"},\"algorithms\":[{\"name\":\"EigenDecomposition\",\"evaluator\":\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator\",\"description\":\"EigenBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"},{\"name\":\"KDE\",\"evaluator\":\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyKDEEvaluator\",\"description\":\"DensityBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"}]}", "dedupeDef": "{\"alertDedupIntervalMin\":\"0\",\"emailDedupIntervalMin\":\"0\"}", "notificationDef": "", "remediationDef": "", "enabled": true } ]'

echo ""
echo "Importing AlertExecutorService for USERPROFILE"
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertExecutorService" \
      -d '[ { "prefix": "alertExecutor", "tags":{ "site":"sandbox", "dataSource":"userProfile", "alertExecutorId" : "userProfileAnomalyDetectionExecutor", "streamName":"userActivity" }, "desc": "user activity data source" } ]'

echo ""
echo "Importing AlertStreamService for USERPROFILE"
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertStreamService" \
     -d '[ { "prefix": "alertStream", "tags": { "streamName": "userActivity", "site":"sandbox", "dataSource":"userProfile" }, "alertExecutorIdList": [ "userProfileAnomalyDetectionExecutor" ] } ]'

## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
