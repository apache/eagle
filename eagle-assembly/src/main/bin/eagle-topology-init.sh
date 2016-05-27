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

echo "Begin to initialize HBase tables ..."

echo ""
echo "Importing sample site ..."
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteDescService" -d '[{"prefix":"eagleSiteDesc","tags":{"site" : "sandbox"}, "enabled": true}]'

echo ""
echo "Importing applications for sample site ..."

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" -d '[{"prefix":"eagleSiteApplication","tags":{"site" : "sandbox", "application":"hdfsAuditLog"}, "enabled": true, "config" : "classification.fs.defaultFS=hdfs://sandbox.hortonworks.com:8020"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" -d '[{"prefix":"eagleSiteApplication","tags":{"site" : "sandbox", "application":"hbaseSecurityLog"}, "enabled": true, "config" : "classification.hbase.zookeeper.property.clientPort=2181\nclassification.hbase.zookeeper.quorum=localhost"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" -d '[{"prefix":"eagleSiteApplication","tags":{"site" : "sandbox", "application":"hiveQueryLog"}, "enabled": true, "config":"classification.accessType=metastoredb_jdbc\nclassification.password=hive\nclassification.user=hive\nclassification.jdbcDriverClassName=com.mysql.jdbc.Driver\nclassification.jdbcUrl=jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true"}]'

echo ""
echo "Importing application definitions ..."
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" -d '[{"prefix":"eagleApplicationDesc","tags":{"application":"hdfsAuditLog"},"description":"HDFS audit log security check application","alias":"HDFS","groupName":"DAM","features":["common","classification","userProfile","metadata"],"config":"{\n\t\"view\": {\n\t\t\"prefix\": \"fileSensitivity\",\n\t\t\"service\": \"FileSensitivityService\",\n\t\t\"keys\": [\n\t\t\t\"filedir\",\n\t\t\t\"sensitivityType\"\n\t\t],\n\t\t\"type\": \"folder\",\n\t\t\"api\": \"hdfsResource\"\n\t}\n}"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" -d '[{"prefix":"eagleApplicationDesc","tags":{"application":"hbaseSecurityLog"},"description":"HBASE audit log security check application","alias":"HBASE","groupName":"DAM","features":["common","classification","userProfile","metadata"],"config":"{\n\t\"view\": {\n\t\t\"prefix\": \"hbaseResourceSensitivity\",\n\t\t\"service\": \"HbaseResourceSensitivityService\",\n\t\t\"keys\": [\n\t\t\t\"hbaseResource\",\n\t\t\t\"sensitivityType\"\n\t\t],\n\t\t\"type\": \"table\",\n\t\t\"api\": {\n\t\t\t\"database\": \"hbaseResource/namespaces\",\n\t\t\t\"table\": \"hbaseResource/tables\",\n\t\t\t\"column\": \"hbaseResource/columns\"\n\t\t},\n\t\t\"mapping\": {\n\t\t\t\"database\": \"namespace\",\n\t\t\t\"table\": \"table\",\n\t\t\t\"column\": \"columnFamily\"\n\t\t}\n\t}\n}"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" -d '[{"prefix":"eagleApplicationDesc","tags":{"application":"hiveQueryLog"},"description":"Hive query log security check application","alias":"HIVE","groupName":"DAM","features":["common","classification","userProfile","metadata"], "config":"{\n\t\"view\": {\n\t\t\"prefix\": \"hiveResourceSensitivity\",\n\t\t\"service\": \"HiveResourceSensitivityService\",\n\t\t\"keys\": [\n\t\t\t\"hiveResource\",\n\t\t\t\"sensitivityType\"\n\t\t],\n\t\t\"type\": \"table\",\n\t\t\"api\": {\n\t\t\t\"database\": \"hiveResource/databases\",\n\t\t\t\"table\": \"hiveResource/tables\",\n\t\t\t\"column\": \"hiveResource/columns\"\n\t\t},\n\t\t\"mapping\": {\n\t\t\t\"database\": \"database\",\n\t\t\t\"table\": \"table\",\n\t\t\t\"column\": \"column\"\n\t\t}\n\t}\n}"}]'

echo ""
echo "Importing feature definitions ..."
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"common"},"description":"Provide the Policy & Alert feature.","version":"v0.3.0"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"classification"},"description":"Sensitivity browser of the data classification.","version":"v0.3.0"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"userProfile"},"description":"Machine learning of the user profile","version":"v0.3.0"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"metadata"},"description":"Stream metadata viewer","version":"v0.3.0"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"metrics"},"description":"Metrics dashboard","version":"v0.3.0"}]'

curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=FeatureDescService" -d '[{"prefix":"eagleFeatureDesc","tags":{"feature":"topology"},"description":"Application topology management feature","version":"v0.4.0"}]'


## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HDFS... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream"},"description":"alert event stream from hdfs audit log"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HDFS... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"application":"hdfsAuditLog","alertExecutorId":"hdfsAuditLogAlertExecutor","streamName":"hdfsAuditLogEventStream"},"description":"alert executor for hdfs audit log event stream"}]'

## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HDFS... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"src"},"attrDescription":"source directory or file, such as /tmp","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"dst"},"attrDescription":"destination directory, such as /tmp","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"host"},"attrDescription":"hostname, such as localhost","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"timestamp"},"attrDescription":"milliseconds of the datetime","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"allowed"},"attrDescription":"true, false or none","attrType":"bool","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"user"},"attrDescription":"process user","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"cmd"},"attrDescription":"file/directory operation, such as getfileinfo, open, listStatus and so on","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSCommandResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"sensitivityType"},"attrDescription":"mark such as AUDITLOG, SECURITYLOG","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hdfs.resolver.HDFSSensitivityTypeResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hdfsAuditLog","streamName":"hdfsAuditLogEventStream","attrName":"securityZone"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":""}]'


#####################################################################
#            Import stream metadata for HBASE
#####################################################################

#### AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HBASE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream"},"description":"alert event stream from hbase security audit log"}]'


#### AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HBASE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"application":"hbaseSecurityLog","alertExecutorId":"hbaseSecurityLogAlertExecutor","streamName":"hbaseSecurityLogEventStream"},"description":"alert executor for hbase security log event stream"}]'


#### AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HBASE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"host"},"attrDescription":"remote ip address to access hbase","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"request"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseRequestResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"status"},"attrDescription":"access status: allowed or denied","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"user"},"attrDescription":"hbase user","attrType":"string","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"timestamp"},"attrDescription":"milliseconds of the datetime","attrType":"long","category":"","attrValueResolver":""},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"scope"},"attrDescription":"the resources which users are then granted specific permissions (Read, Write, Execute, Create, Admin) against","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseMetadataResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"action"},"attrDescription":"action types, such as read, write, create, execute, and admin","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseActionResolver"},{"prefix":"alertStreamSchema","tags":{"application":"hbaseSecurityLog","streamName":"hbaseSecurityLogEventStream","attrName":"sensitivityType"},"attrDescription":"","attrType":"string","category":"","attrValueResolver":"org.apache.eagle.service.security.hbase.resolver.HbaseSensitivityTypeResolver"}]'


#####################################################################
#            Import stream metadata for HIVE
#####################################################################

## AlertStreamService: alert streams generated from data source
echo ""
echo "Importing AlertStreamService for HIVE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" -d '[{"prefix":"alertStream","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream"},"description":"alert event stream from hive query"}]'

## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for HIVE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" -d '[{"prefix":"alertExecutor","tags":{"application":"hiveQueryLog","alertExecutorId":"hiveAccessAlertByRunningJob","streamName":"hiveAccessLogStream"},"description":"alert executor for hive query log event stream"}]'

## AlertStreamSchemaServiceService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for HIVE... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"process user","attrValueResolver":"","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"user"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"hive sql command, such as SELECT, INSERT and DELETE","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveCommandResolver","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"command"}},{"prefix":"alertStreamSchema","category":"","attrType":"long","attrDescription":"milliseconds of the datetime","attrValueResolver":"","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"timestamp"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"/database/table/column or /database/table/*","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveMetadataResolver","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"resource"}},{"prefix":"alertStreamSchema","category":"","attrType":"string","attrDescription":"mark such as PHONE_NUMBER","attrValueResolver":"org.apache.eagle.service.security.hive.resolver.HiveSensitivityTypeResolver","tags":{"application":"hiveQueryLog","streamName":"hiveAccessLogStream","attrName":"sensitivityType"}}]'

#####################################################################
#            Import stream metadata for UserProfile
#####################################################################


echo ""
echo "Importing AlertDefinitionService for USERPROFILE"
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertDefinitionService" \
     -d '[ { "prefix": "alertdef", "tags": { "site": "sandbox", "application": "userProfile", "alertExecutorId": "userProfileAnomalyDetectionExecutor", "policyId": "userProfile", "policyType": "MachineLearning" }, "description": "user profile anomaly detection", "policyDef": "{\"type\":\"MachineLearning\",\"alertContext\":{\"site\":\"sandbox\",\"application\":\"userProfile\",\"component\":\"testComponent\",\"description\":\"ML based user profile anomaly detection\",\"severity\":\"WARNING\",\"notificationByEmail\":\"true\"},\"algorithms\":[{\"name\":\"EigenDecomposition\",\"evaluator\":\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator\",\"description\":\"EigenBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"},{\"name\":\"KDE\",\"evaluator\":\"org.apache.eagle.security.userprofile.impl.UserProfileAnomalyKDEEvaluator\",\"description\":\"DensityBasedAnomalyDetection\",\"features\":\"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete, setOwner, fsck\"}]}", "dedupeDef": "{\"alertDedupIntervalMin\":\"0\",\"emailDedupIntervalMin\":\"0\"}", "notificationDef": "", "remediationDef": "", "enabled": true } ]'

echo ""
echo "Importing AlertExecutorService for USERPROFILE"
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertExecutorService" \
      -d '[ { "prefix": "alertExecutor", "tags":{ "site":"sandbox", "application":"userProfile", "alertExecutorId" : "userProfileAnomalyDetectionExecutor", "streamName":"userActivity" }, "description": "user activity data source" } ]'

echo ""
echo "Importing AlertStreamService for USERPROFILE"
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H "Content-Type: application/json"  "http://$EAGLE_SERVICE_HOST:$EAGLE_SERVICE_PORT/eagle-service/rest/entities?serviceName=AlertStreamService" \
     -d '[ { "prefix": "alertStream", "tags": { "streamName": "userActivity", "site":"sandbox", "application":"userProfile" }, "alertExecutorIdList": [ "userProfileAnomalyDetectionExecutor" ] } ]'

#####################################################################
#     Import notification plugin configuration into Eagle Service   #
#####################################################################

## AlertNotificationService : schema for notifcation plugin configuration
echo ""
echo "Importing notification plugin configurations ... "
curl -silent -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertNotificationService" \
 -d '
 [
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "email"
       },
       "className": "org.apache.eagle.notification.plugin.AlertEmailPlugin",
       "description": "send alert to email",
       "enabled":true,
       "fields": "[{\"name\":\"sender\"},{\"name\":\"recipients\"},{\"name\":\"subject\"}]"
     },
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "kafka"
       },
       "className": "org.apache.eagle.notification.plugin.AlertKafkaPlugin",
       "description": "send alert to kafka bus",
       "enabled":true,
       "fields": "[{\"name\":\"kafka_broker\",\"value\":\"sandbox.hortonworks.com:6667\"},{\"name\":\"topic\"}]"
     },
     {
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "eagleStore"
       },
       "className": "org.apache.eagle.notification.plugin.AlertEagleStorePlugin",
       "description": "send alert to eagle store",
       "enabled":true
     }
 ]
 '

## Finished
echo ""
echo "Finished initialization for eagle topology"

exit 0
