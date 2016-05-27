#!/usr/bin/env bash
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

# EAGLE_SERVICE_HOST, default is `hostname -f`
export EAGLE_SERVICE_HOST=localhost
# EAGLE_SERVICE_PORT, default is 9099
export EAGLE_SERVICE_PORT=9099
# EAGLE_SERVICE_USER
export EAGLE_SERVICE_USER=admin
# EAGLE_SERVICE_PASSWORD
export EAGLE_SERVICE_PASSWD=secret
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=SiteApplicationService" \
  -d '
  [
     {
        "tags":{
           "site":"sandbox",
           "application":"maprFSAuditLog"
        },
        "enabled": true,
        "config": "classification.fs.defaultFS=maprfs:///"
     }
  ]
  '
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=ApplicationDescService" \
  -d '
  [
     {
        "tags":{
           "application":"maprFSAuditLog"
        },
        "description":"mapr AuditLog Monitoring",
        "alias":"MapRFSAuditLogMonitor",
        "groupName":"MapR",
        "features":["common","metadata", "classification"],
	"config":"{\n\t\"view\": {\n\t\t\"prefix\": \"fileSensitivity\",\n\t\t\"service\": \"FileSensitivityService\",\n\t\t\"keys\": [\n\t\t\t\"filedir\",\n\t\t\t\"sensitivityType\"\n\t\t],\n\t\t\"type\": \"folder\",\n\t\t\"api\": \"hdfsResource\"\n\t}\n}"
     }
  ]
  '



## AlertStreamService
echo ""
echo "Importing AlertStreamService for MapRFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamService" \
 -d '
 [
    {
       "tags":{
          "application":"maprFSAuditLog",
          "streamName":"maprFSAuditLogEventStream"
       },
       "description":"mapr fs audit log data source stream"
    }
 ]
 '
## AlertExecutorService: what alert streams are consumed by alert executor
echo ""
echo "Importing AlertExecutorService for MapRFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
 "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertExecutorService" \
 -d '
 [
    {
       "tags":{
          "application":"maprFSAuditLog",
          "alertExecutorId":"maprFSAuditLogAlertExecutor",
          "streamName":"maprFSAuditLogEventStream"
       },
       "description":"executor for mapr fs audit log stream"
    }
 ]
 '
## AlertStreamSchemaService: schema for event from alert stream
echo ""
echo "Importing AlertStreamSchemaService for MapRFS... "
curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -X POST -H 'Content-Type:application/json' \
"http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" \
 -d '
 [
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "status"
       },
       "attrDescription": "the status of action result,  numeric codes other than 0 for success can appear, for more info, check: http://doc.mapr.com/display/MapR/Status+Codes+That+Can+Appear+in+Audit+Logs",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.MAPRStatusCodeResolver"
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "cmd"
       },
       "attrDescription": "file/directory operation, such as MKDIR, LOOKUP",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.MAPRFSCommandResolver"
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "volume"
       },
       "attrDescription": "volume name in mapr",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "dst"
       },
       "attrDescription": "destination file or directory, such as /tmp",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSResourceResolver"
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "src"
       },
       "attrDescription": "source file or directory, such as /tmp",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "HDFSResourceResolver"
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "host"
       },
       "attrDescription": "hostname, such as localhost",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "sensitivityType"
       },
       "attrDescription": "sensitivity type of of target file/folder, eg: mark such as AUDITLOG, SECURITYLOG",
       "attrType": "string",
       "category": "",
       "attrValueResolver": "org.apache.eagle.service.security.hdfs.resolver.HDFSSensitivityTypeResolver"
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "securityZone"
       },
       "attrDescription": "",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "user"
       },
       "attrDescription": "process user",
       "attrType": "string",
       "category": "",
       "attrValueResolver": ""
    },
    {
       "tags": {
          "application": "maprFSAuditLog",
          "streamName": "maprFSAuditLogEventStream",
          "attrName": "timestamp"
       },
       "attrDescription": "milliseconds of the datetime",
       "attrType": "long",
       "category": "",
       "attrValueResolver": ""
    }

 ]
 '
## Finished
echo ""
echo "Finished initialization for eagle topology"
