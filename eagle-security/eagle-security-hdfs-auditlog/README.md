<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Development in IDE

## 1. Start eagle-server
In IDE, configure the following main class and program arguments

org.apache.eagle.server.ServerMain server src/main/resources/configuration.yml

## 2. Start alert engine

### 2.1 Create new site

http://localhost:9090/rest/sites POST
```
{
"siteId" : "testsite",
"siteName" :"testsite",
"description" : "test description",
"context" : {}
}
```

###n2.2 Create logic alert engine topology

http://localhost:9090/rest/metadata/topologies POST
```
{
   "name": "alertUnitTopology_1",
   "numOfSpout": 1,
   "numOfAlertBolt": 10,
   "numOfGroupBolt": 4,
   "spoutId": "alertEngineSpout",
   "groupNodeIds": [
      "streamRouterBolt0",
      "streamRouterBolt1",
      "streamRouterBolt2",
      "streamRouterBolt3"
   ],
   "alertBoltIds": [
      "alertBolt0",
      "alertBolt1",
      "alertBolt2",
      "alertBolt3",
      "alertBolt4",
      "alertBolt5",
      "alertBolt6",
      "alertBolt7",
      "alertBolt8",
      "alertBolt9"
   ],
   "pubBoltId": "alertPublishBolt",
   "spoutParallelism": 1,
   "groupParallelism": 1,
   "alertParallelism": 1
}
```

### 2.3 Install alert engine application
Please reference eagle-core/eagle-alert-parent/eagle-alert-app/src/main/resources/META-INF/providers/org.apache.eagle.alert.app.AlertUnitTopologyAppProvider.xml for
complete configuration.

http://localhost:9090/rest/apps/install POST
```
{
"siteId" : "testsite",
"appType" : "AlertUnitTopologyApp",
"mode" : "LOCAL",
"configuration" : {
  }
}
```

### 2.4 Run alert engine
Please use correct uuid

http://localhost:9090/rest/apps/start POST
```
{
"uuid": "dc61c4b8-f60d-4d95-bfd7-f6b07382a3f3",
"appId": "AlertUnitTopologyApp－testsite"
}
```

## 3 Start Hdfs audit log monitoring application

### 3.1 Install HdfsAuditLog app

http://localhost:9090/rest/apps/install POST
```
{
"siteId" : "testsite",
"appType" : "HdfsAuditLogApplication",
"mode" : "LOCAL",
"configuration" : {
  "dataSourceConfig.topic" :"hdfs_audit_log"}
}
```

### 3.2 Start HdfsAuditLog app
Please use correct uuid

http://localhost:9090/rest/apps/start POST
```
{
"uuid": "dc61c4b8-f60d-4d95-bfd7-f6b07382a3f3",
"appId": "HdfsAuditLogApplication－testsite"
}
```

## 4 Check
### 4.1 Check if alert data source is created
http://localhost:9090/rest/metadata/datasources GET

### 4.2 Check if alert stream is creatd
http://localhost:9090/rest/metadata/streams GET

## 5 Create alert policy and verify alert
### 5.1 create one policy

http://localhost:9090/rest/metadata/policies POST
```
{
   "name": "hdfsPolicy",
   "description": "hdfsPolicy",
   "inputStreams": [
      "hdfs_audit_log_enriched_stream"
   ],
   "outputStreams": [
      "hdfs_audit_log_enriched_stream_out"
   ],
   "definition": {
      "type": "siddhi",
      "value": "from hdfs_audit_log_enriched_stream[user=='hadoop'] select * insert into hdfs_audit_log_enriched_stream_out"
   },
   "partitionSpec": [
      {
         "streamId": "hdfs_audit_log_enriched_stream",
         "type": "GROUPBY",
         "columns" : [
            "user"
         ]
      }
   ],
   "parallelismHint": 2
}
```

### 5.2 Create alert publishment
```
{
	"name":"hdfs_audit_log_enriched_stream_out",
	"type":"org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher",
	"policyIds": [
		"hdfsPolicy"
	],
	"properties": {
	  "subject":"alert when user is hadoop",
	  "template":"",
	  "sender": "eagle@apache.org",
	  "recipients": "eagle@apache.org",
	  "mail.smtp.host":"",
	  "connection": "plaintext",
	  "mail.smtp.port": "25"
	},
	"dedupIntervalMin" : "PT1M",
	"serializer" : "org.apache.eagle.alert.engine.publisher.impl.StringEventSerializer"
}
```

### 5.3 Send message and verify alert
./kafka-console-producer.sh --topic hdfs_audit_log --broker-list sandbox.hortonworks.com:6667

2015-04-24 12:51:31,798 INFO FSNamesystem.audit: allowed=true	ugi=hdfs (auth:SIMPLE)	ip=/10.0.2.15	cmd=getfileinfo	src=/apps/hbase/data	dst=null	perm=null	proto=rpc
